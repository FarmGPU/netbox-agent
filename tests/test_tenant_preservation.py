"""
Tenancy is NetBox-owned: a sync must never write `tenant` on an existing device.

netbox-agent used to infer tenant from running systemd services
(runpod/moosefs -> "runpod", else -> "farmgpu") and rewrite the field every
cycle. The call also sat outside the `network_only` guard, so the 4-hour
network timer re-fired it. At lax01 on 2026-09-11, 12 devices corrected the
previous day were back to `tenant=farmgpu`, each with a `last_agent_sync`
inside the preceding 20 minutes.

The guard has to drive the real `netbox_create_or_update` flow *past* the line
the old `_sync_tenant(server)` call occupied — a test that aborts earlier
passes just as happily with the inference still in place. So the collaborators
below are stubbed rather than made to raise, the run goes end to end, and the
assertion is that nothing ever assigns to `device.tenant`.

Both the daily sync and the `network_only` timer are covered: the timer is the
path that actually did the damage.
"""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# Pre-mock config to avoid import-time sys.argv parsing (see test_cable_orphan)
_mock_config_module = MagicMock()
_mock_config_module.config = SimpleNamespace(
    update_all=False,
    update_network=False,
    register=False,
    network=SimpleNamespace(
        ignore_interfaces="(dummy.*|docker.*)",
        ignore_ips="^(127\\.0\\.0\\..*)",
        ipmi=False,
        lldp=None,
        nic_id="name",
        primary_mac="temp",
    ),
)
_mock_config_module.netbox_instance = MagicMock(name="nb")
sys.modules.setdefault("netbox_agent.config", _mock_config_module)
sys.modules.setdefault("netbox_agent.misc", MagicMock())
sys.modules.setdefault("netbox_agent.ethtool", MagicMock())
sys.modules.setdefault("netbox_agent.lldp", MagicMock())
sys.modules.setdefault("netbox_agent.ipmi", MagicMock())

from netbox_agent.server import ServerBase  # noqa: E402


class _Device:
    """A pynetbox device record that records every write to `tenant`.

    `save()` is deliberately not the thing under assertion — the real flow
    saves several times before it reaches the old tenant call site (custom
    fields, asset tag, the `if update:` block), so a bare
    `save.assert_not_called()` would only ever pass by aborting early.
    """

    def __init__(self, tenant):
        object.__setattr__(self, "tenant_writes", [])
        object.__setattr__(self, "saves", 0)
        object.__setattr__(self, "tenant", tenant)
        self.id = 42
        self.name = "ash030"
        self.serial = "J9KFT34"
        self.asset_tag = "ash030"
        self.tags = []
        self.custom_fields = {"last_agent_sync": "2026-09-10T00:00:00+00:00"}
        self.status = SimpleNamespace(value="active")
        self.platform = None
        self.primary_ip4 = None
        self.oob_ip = None

    def __setattr__(self, name, value):
        if name == "tenant":
            self.tenant_writes.append(value)
        object.__setattr__(self, name, value)

    def save(self):
        object.__setattr__(self, "saves", self.saves + 1)


def _make_config():
    return SimpleNamespace(
        update_old_devices=False,
        purge_old_devices=False,
        register=False,
        update_all=False,
        update_network=False,
        update_psu=False,
        update_location=False,
        preserve_tags=True,
        expansion_as_device=False,
        modules=False,
        sync_cadence=86400,
        virtual=SimpleNamespace(hypervisor=None, list_guests_cmd=None),
    )


def _make_nb(device):
    nb = MagicMock(name="nb")

    def devices_get(*args, **kwargs):
        # The flow re-fetches the device by id twice near the end; those must
        # keep returning the tracked record. The expansion lookup goes by
        # serial and must find nothing.
        if "serial" in kwargs:
            return None
        return device

    nb.dcim.devices.get.side_effect = devices_get
    nb.ipam.ip_addresses.filter.return_value = []
    return nb


def _make_server(device, inferred_tenant):
    """A ServerBase whose collaborators all no-op, so the sync runs to the end
    without touching the host or NetBox."""
    server = ServerBase.__new__(ServerBase)

    # Local facts match what NetBox already holds, so nothing but the
    # agent-owned custom fields comes out dirty.
    server.get_hostname = MagicMock(return_value=device.name)
    server._get_best_serial = MagicMock(return_value=device.serial)
    server.get_asset_tag = MagicMock(return_value=device.asset_tag)
    server.device_platform = device.platform
    server.tags = []
    server.nb_tags = []
    server.custom_fields = {}

    # A tenant driver that *would* hand back the wrong answer if anything
    # still applied it to an existing device.
    server.get_netbox_tenant = MagicMock(return_value=inferred_tenant)
    server.get_netbox_datacenter = MagicMock(return_value=None)
    server.get_netbox_rack = MagicMock(return_value=None)
    server.is_blade = MagicMock(return_value=False)
    server.get_netbox_server = MagicMock(return_value=device)
    server._netbox_create_server = MagicMock()

    server._ensure_required_custom_fields = MagicMock()
    server._get_chassis_serial = MagicMock(return_value=None)
    server._get_bmc_mac = MagicMock(return_value=None)
    server._refine_role = MagicMock()
    server._is_proxmox_host = MagicMock(return_value=False)
    server.own_expansion_slot = MagicMock(return_value=False)
    server.get_expansion_service_tag = MagicMock(return_value=None)
    server._get_default_gateway_interface = MagicMock(return_value=None)
    server._resolve_primary_ip4 = MagicMock(return_value=(None, False))
    return server


@pytest.mark.parametrize("network_only", [False, True])
def test_existing_device_tenant_is_never_written(network_only):
    existing_tenant = SimpleNamespace(id=10, name="hosted.ai")
    inferred_tenant = SimpleNamespace(id=20, name="farmgpu")

    device = _Device(tenant=existing_tenant)
    server = _make_server(device, inferred_tenant)

    with patch("netbox_agent.server.nb", _make_nb(device)), patch(
        "netbox_agent.server.ServerNetwork"
    ):
        server.netbox_create_or_update(_make_config(), network_only=network_only)

    # Execution reached and passed the old `_sync_tenant(server)` call site,
    # which sat immediately after `_refine_role`, and ran on to the end.
    server._refine_role.assert_called_once()
    server._resolve_primary_ip4.assert_called_once()
    # ...and did its ordinary work on the way, so this is a real sync.
    assert device.saves >= 1

    # The parametrization exercises genuinely different paths: the hardware
    # half of the sync is inside `if not network_only:`, the old tenant write
    # was not.
    assert server._is_proxmox_host.called is (not network_only)

    assert device.tenant_writes == []
    assert device.tenant is existing_tenant

"""
primary_ip4 must converge on the management address, not merely be filled
when empty.

A device enrolled BMC-first has primary_ip4 set to its OOB address by
bmc-scan.  That address is legitimately assigned to the IPMI interface, so
the "no longer assigned" staleness check never clears it, and the old
"fill only when empty" branch never ran -- the BMC address stayed primary
forever.  nb_inventory derives ansible_host from primary_ip, so Ansible
then targets the BMC instead of the OS.

Observed on rocoto05-108l / rocoto16-108y (2026-08-28).
"""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

# Pre-mock config to avoid import-time sys.argv parsing (see test_cable_orphan)
_mock_config_module = MagicMock()
_mock_config_module.config = SimpleNamespace(
    update_all=True, update_network=True, register=False,
    network=SimpleNamespace(
        ignore_interfaces="(dummy.*)", ignore_ips="^(127\\.0\\.0\\..*)",
        ipmi=False, lldp=None, nic_id="name", primary_mac="temp",
    ),
)
_mock_config_module.netbox_instance = MagicMock(name="nb")
sys.modules.setdefault("netbox_agent.config", _mock_config_module)
sys.modules.setdefault("netbox_agent.misc", MagicMock())
sys.modules.setdefault("netbox_agent.ethtool", MagicMock())
sys.modules.setdefault("netbox_agent.lldp", MagicMock())
sys.modules.setdefault("netbox_agent.ipmi", MagicMock())

from netbox_agent.server import ServerBase  # noqa: E402

resolve = ServerBase._resolve_primary_ip4


def _ip(ip_id, iface, family=4):
    return SimpleNamespace(
        id=ip_id,
        assigned_object=SimpleNamespace(display=iface),
        family=SimpleNamespace(value=family),
    )


BMC = _ip(3075, "IPMI")        # 10.100.192.150/32
MGMT = _ip(2937, "mgmt0")      # 10.100.194.59/24
ALL = [BMC, MGMT]
ATTACHED = {BMC.id, MGMT.id}


def test_wrong_but_attached_bmc_ip_is_corrected():
    """The rocoto05 case: BMC address is primary AND attached -> must move."""
    new, changed = resolve(BMC, ATTACHED, ALL, "mgmt0")
    assert new == MGMT.id
    assert changed is True


def test_empty_primary_is_filled():
    new, changed = resolve(None, ATTACHED, ALL, "mgmt0")
    assert new == MGMT.id
    assert changed is True


def test_already_correct_is_a_noop():
    """Idempotency: a second run must not report a change."""
    new, changed = resolve(MGMT, ATTACHED, ALL, "mgmt0")
    assert new == MGMT.id
    assert changed is False


def test_no_mgmt_interface_leaves_primary_alone():
    """DPUs / switches: no default-gateway IPv4, so the OOB address stands."""
    new, changed = resolve(BMC, {BMC.id}, [BMC], None)
    assert new == BMC.id
    assert changed is False


def test_detached_primary_is_cleared_when_no_mgmt_ip_exists():
    """The dangling case must still be nulled, not left pointing at nothing."""
    stale = _ip(9999, "gone")
    new, changed = resolve(stale, {BMC.id}, [BMC], None)
    assert new is None
    assert changed is True


def test_detached_primary_is_replaced_when_mgmt_ip_exists():
    stale = _ip(9999, "gone")
    new, changed = resolve(stale, ATTACHED, ALL, "mgmt0")
    assert new == MGMT.id
    assert changed is True


def test_ipv6_on_the_mgmt_interface_is_ignored():
    v6 = _ip(4242, "mgmt0", family=6)
    new, changed = resolve(BMC, {BMC.id, v6.id}, [BMC, v6], "mgmt0")
    assert new == BMC.id      # no v4 on mgmt0 -> nothing to converge on
    assert changed is False

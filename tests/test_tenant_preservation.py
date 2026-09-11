import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

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


class _StopBeforeFurtherSync(Exception):
    pass


@pytest.mark.parametrize("network_only", [False, True])
def test_existing_device_tenant_is_not_reassigned(network_only):
    existing_tenant = SimpleNamespace(id=10, name="hosted.ai")
    inferred_tenant = SimpleNamespace(id=20, name="farmgpu")

    nb_device = MagicMock(name="nb-device")
    nb_device.tenant = existing_tenant

    server = ServerBase.__new__(ServerBase)
    server.get_netbox_datacenter = MagicMock(return_value=None)
    server.get_netbox_rack = MagicMock(return_value=None)
    server.get_netbox_tenant = MagicMock(return_value=inferred_tenant)
    server.is_blade = MagicMock(return_value=False)
    server.get_netbox_server = MagicMock(return_value=nb_device)
    server._netbox_create_server = MagicMock()
    server._ensure_required_custom_fields = MagicMock(side_effect=_StopBeforeFurtherSync)

    config = SimpleNamespace(update_old_devices=False, purge_old_devices=False)

    with pytest.raises(_StopBeforeFurtherSync):
        server.netbox_create_or_update(config, network_only=network_only)

    assert nb_device.tenant is existing_tenant
    nb_device.save.assert_not_called()
    server._netbox_create_server.assert_not_called()

"""
Unit tests for the BlueField DPU vendor class.

These tests mock netbox_agent.config (which parses sys.argv at import) and
the IPMI module so we can instantiate BlueFieldHost in isolation.
"""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, mock_open

# ---------------------------------------------------------------------------
# Pre-mock netbox_agent.config so it doesn't parse sys.argv or call pynetbox
# ---------------------------------------------------------------------------
_mock_nb = MagicMock()
_mock_config = SimpleNamespace(
    register=False,
    update_all=False,
    debug=False,
    hostname_cmd=None,
    preserve_tags=False,
    log_level="debug",
    netbox=SimpleNamespace(url="http://test", token="test", ssl_verify=True, ssl_ca_certs_file=None),
    device=SimpleNamespace(
        platform=None, tags="", custom_fields="", blade_role="Blade",
        chassis_role="Server Chassis", server_role="Server",
        default_owner="FarmGPU", asset_tag_cmd=None,
    ),
    dpu=SimpleNamespace(parent_device=None, device_bay=None, netbox_name=None),
    tenant=SimpleNamespace(driver=None, driver_file=None, regex=None),
    datacenter_location=SimpleNamespace(driver=None, driver_file=None, regex=None),
    rack_location=SimpleNamespace(driver=None, driver_file=None, regex=None),
    network=SimpleNamespace(
        ignore_interfaces="(dummy.*|docker.*)", ignore_ips="^(127\\.0\\.0\\..*)",
        ipmi=True, lldp=None, nic_id="name", primary_mac="temp",
    ),
)

_mock_config_module = MagicMock()
_mock_config_module.config = _mock_config
_mock_config_module.netbox_instance = _mock_nb
sys.modules["netbox_agent.config"] = _mock_config_module

# Also mock misc to avoid running system commands
_mock_misc = MagicMock()
_mock_misc.is_tool = MagicMock(return_value=False)
_mock_misc.create_netbox_tags = MagicMock(return_value=[])
_mock_misc.get_device_role = MagicMock()
_mock_misc.get_device_type = MagicMock()
_mock_misc.get_device_platform = MagicMock()
sys.modules["netbox_agent.misc"] = _mock_misc

# Mock IPMI module — BlueField has no IPMI, but ServerBase.__init__ tries it
_mock_ipmi_module = MagicMock()
_mock_ipmi_instance = MagicMock()
_mock_ipmi_instance.parse.return_value = None  # IPMI fails, as expected on BF3
_mock_ipmi_module.IPMI = MagicMock(return_value=_mock_ipmi_instance)
sys.modules["netbox_agent.ipmi"] = _mock_ipmi_module

# Now safe to import
from netbox_agent.vendors.nvidia import BlueFieldHost  # noqa: E402


# ---------------------------------------------------------------------------
# DMI fixture: minimal valid output as BlueField BSP might produce.
# ServerBase.__init__ reads Chassis/System/Baseboard/BIOS dicts.
# ---------------------------------------------------------------------------
_DMI_FIXTURE = {
    "0x0000": {
        "DMIType": 0, "DMIName": "BIOS Information",
        "Vendor": "NVIDIA", "Version": "BSP-4.6.0",
    },
    "0x0001": {
        "DMIType": 1, "DMIName": "System Information",
        "Manufacturer": "NVIDIA", "Product Name": "BlueField-3 B3220",
        "Serial Number": "MT2522XZ04ZM", "UUID": "abc-def",
    },
    "0x0002": {
        "DMIType": 2, "DMIName": "Base Board Information",
        "Manufacturer": "NVIDIA", "Product Name": "BlueField-3",
        "Serial Number": "MT2522XZ04ZM",
    },
    "0x0003": {
        "DMIType": 3, "DMIName": "Chassis Information",
        "Manufacturer": "NVIDIA", "Type": "Other",
        "Asset Tag": "Not Specified",
    },
}


_FAKE_OOB_MAC = "d8:94:24:20:10:08"


def test_is_not_blade():
    h = BlueFieldHost(dmi=_DMI_FIXTURE)
    assert h.is_blade() is False
    assert h.get_blade_slot() is None
    assert h.get_chassis_name() is None


def test_manufacturer_overridden_to_nvidia():
    h = BlueFieldHost(dmi=_DMI_FIXTURE)
    assert h.manufacturer == "NVIDIA"


def test_service_tag_reads_dmi_system_serial():
    """BF3 BSP populates DMI Type 1 Serial Number with the Mellanox board serial."""
    h = BlueFieldHost(dmi=_DMI_FIXTURE)
    assert h.get_service_tag() == "MT2522XZ04ZM"


def test_get_bmc_mac_reads_oob_net0():
    """_get_bmc_mac should read /sys/class/net/oob_net0/address."""
    with patch("builtins.open", mock_open(read_data=_FAKE_OOB_MAC + "\n")):
        h = BlueFieldHost(dmi=_DMI_FIXTURE)
        # __init__ already called _get_bmc_mac via super(); just verify direct call
        assert h._get_bmc_mac() == _FAKE_OOB_MAC.upper()


def test_init_populates_custom_field_from_oob_mac():
    """After __init__, custom_fields['bmc_mac_address'] should be set."""
    with patch("builtins.open", mock_open(read_data=_FAKE_OOB_MAC + "\n")):
        h = BlueFieldHost(dmi=_DMI_FIXTURE)
        assert h.custom_fields.get("bmc_mac_address") == _FAKE_OOB_MAC.upper()


def test_get_bmc_mac_missing_iface_returns_none():
    """If oob_net0 is absent, _get_bmc_mac returns None (and logs warning)."""
    with patch("builtins.open", side_effect=FileNotFoundError("no such interface")):
        h = BlueFieldHost(dmi=_DMI_FIXTURE)
        assert h._get_bmc_mac() is None


def test_get_bmc_mac_rejects_all_zero():
    """An all-zero MAC is a misconfig signal — treat as unavailable."""
    with patch("builtins.open", mock_open(read_data="00:00:00:00:00:00\n")):
        h = BlueFieldHost(dmi=_DMI_FIXTURE)
        assert h._get_bmc_mac() is None


def test_create_server_refuses():
    """Agent must never create a DPU device — that path is bmc-api only."""
    h = BlueFieldHost(dmi=_DMI_FIXTURE)
    import pytest
    with pytest.raises(RuntimeError, match="enrich-only"):
        h._netbox_create_server(datacenter=None, tenant=None, rack=None)


# ---------------------------------------------------------------------------
# Parent+bay fallback lookup
# ---------------------------------------------------------------------------

def _reset_dpu_config(parent_device=None, device_bay=None, netbox_name=None):
    """Reset config.dpu to known values for a single test."""
    _mock_config.dpu = SimpleNamespace(
        parent_device=parent_device,
        device_bay=device_bay,
        netbox_name=netbox_name,
    )


def test_get_netbox_server_skips_fallback_when_super_finds_match():
    """If the base class's chain matches, skip the parent+bay query entirely."""
    _reset_dpu_config(parent_device="anaheim04-100x", device_bay="DPU-0")
    matched = MagicMock(name="matched_device", id=42)
    with patch("builtins.open", mock_open(read_data=_FAKE_OOB_MAC + "\n")):
        h = BlueFieldHost(dmi=_DMI_FIXTURE)
        with patch.object(BlueFieldHost.__mro__[1], "get_netbox_server", return_value=matched):
            result = h.get_netbox_server()
    assert result is matched
    # Confirm we did not consult device_bays at all when super matched.
    assert not _mock_nb.dcim.device_bays.get.called


def test_get_netbox_server_returns_none_without_parent_bay_config():
    """First-run skeletal + no dpu.* config = unmatchable (and we don't crash)."""
    _reset_dpu_config()  # all None
    with patch("builtins.open", mock_open(read_data=_FAKE_OOB_MAC + "\n")):
        h = BlueFieldHost(dmi=_DMI_FIXTURE)
        with patch.object(BlueFieldHost.__mro__[1], "get_netbox_server", return_value=None):
            result = h.get_netbox_server()
    assert result is None


def test_get_netbox_server_parent_bay_fallback_matches_installed_device():
    """First-run skeletal + dpu.* config set: lookup by parent device_bay."""
    _reset_dpu_config(parent_device="anaheim04-100x", device_bay="DPU-0")
    installed = SimpleNamespace(id=99)
    fake_bay = SimpleNamespace(installed_device=installed)
    full_device = MagicMock(name="full_device", id=99)
    _mock_nb.reset_mock()
    _mock_nb.dcim.device_bays.get.return_value = fake_bay
    _mock_nb.dcim.devices.get.return_value = full_device

    with patch("builtins.open", mock_open(read_data=_FAKE_OOB_MAC + "\n")):
        h = BlueFieldHost(dmi=_DMI_FIXTURE)
        with patch.object(BlueFieldHost.__mro__[1], "get_netbox_server", return_value=None):
            result = h.get_netbox_server()

    _mock_nb.dcim.device_bays.get.assert_called_once_with(
        device="anaheim04-100x", name="DPU-0",
    )
    _mock_nb.dcim.devices.get.assert_called_once_with(99)
    assert result is full_device


def test_get_netbox_server_returns_none_when_bay_empty():
    """Parent has a DPU-0 bay but nothing installed → return None, don't crash."""
    _reset_dpu_config(parent_device="anaheim04-100x", device_bay="DPU-0")
    empty_bay = SimpleNamespace(installed_device=None)
    _mock_nb.reset_mock()
    _mock_nb.dcim.device_bays.get.return_value = empty_bay

    with patch("builtins.open", mock_open(read_data=_FAKE_OOB_MAC + "\n")):
        h = BlueFieldHost(dmi=_DMI_FIXTURE)
        with patch.object(BlueFieldHost.__mro__[1], "get_netbox_server", return_value=None):
            result = h.get_netbox_server()

    assert result is None
    assert not _mock_nb.dcim.devices.get.called


# ---------------------------------------------------------------------------
# Hostname override
# ---------------------------------------------------------------------------

def test_get_hostname_uses_dpu_netbox_name_when_set():
    """dpu.netbox_name pins the NetBox Device name across runs."""
    _reset_dpu_config(netbox_name="anaheim04-100x-dpu0")
    h = BlueFieldHost(dmi=_DMI_FIXTURE)
    assert h.get_hostname() == "anaheim04-100x-dpu0"


def test_get_hostname_falls_back_to_socket_when_unset():
    """No dpu.netbox_name → behave like ServerBase (socket.gethostname)."""
    _reset_dpu_config()  # all None
    h = BlueFieldHost(dmi=_DMI_FIXTURE)
    with patch("netbox_agent.vendors.nvidia.socket.gethostname", return_value="fallback-host"):
        assert h.get_hostname() == "fallback-host"


# ---------------------------------------------------------------------------
# Post-sync skeletal tag cleanup
# ---------------------------------------------------------------------------

def _make_server_mock(name, serial, bmc_mac, tag_names):
    """Build a pynetbox-like server mock for tag-cleanup tests."""
    server = MagicMock()
    server.name = name
    server.serial = serial
    server.custom_fields = {"bmc_mac_address": bmc_mac} if bmc_mac else {}
    server.tags = [
        SimpleNamespace(id=i, name=tn) for i, tn in enumerate(tag_names, start=1)
    ]
    return server


def test_drop_skeletal_tag_when_fully_enriched():
    """Both serial AND bmc_mac populated → drop dpu-needs-bmc-scan tag."""
    _reset_dpu_config()
    server = _make_server_mock(
        name="anaheim04-100x-dpu0",
        serial="MT2522XZ04ZM",
        bmc_mac=_FAKE_OOB_MAC.upper(),
        tag_names=["dpu-needs-bmc-scan", "managed-by-agent"],
    )
    h = BlueFieldHost(dmi=_DMI_FIXTURE)
    with patch.object(h, "get_netbox_server", return_value=server):
        h._drop_skeletal_tag_if_enriched()

    # Remaining tag list should be just [managed-by-agent]'s id.
    assert server.tags == [2]
    server.save.assert_called_once()


def test_keep_skeletal_tag_when_serial_missing():
    """No serial yet → keep the marker so downstream tooling sees pending."""
    _reset_dpu_config()
    server = _make_server_mock(
        name="anaheim04-100x-dpu0", serial="",
        bmc_mac=_FAKE_OOB_MAC.upper(),
        tag_names=["dpu-needs-bmc-scan"],
    )
    h = BlueFieldHost(dmi=_DMI_FIXTURE)
    with patch.object(h, "get_netbox_server", return_value=server):
        h._drop_skeletal_tag_if_enriched()

    server.save.assert_not_called()


def test_keep_skeletal_tag_when_bmc_mac_missing():
    """No BMC MAC yet → keep the marker."""
    _reset_dpu_config()
    server = _make_server_mock(
        name="anaheim04-100x-dpu0", serial="MT2522XZ04ZM",
        bmc_mac=None, tag_names=["dpu-needs-bmc-scan"],
    )
    h = BlueFieldHost(dmi=_DMI_FIXTURE)
    with patch.object(h, "get_netbox_server", return_value=server):
        h._drop_skeletal_tag_if_enriched()

    server.save.assert_not_called()


def test_drop_tag_noop_when_tag_absent():
    """Already-enriched device with no skeletal tag → no save."""
    _reset_dpu_config()
    server = _make_server_mock(
        name="anaheim04-100x-dpu0", serial="MT2522XZ04ZM",
        bmc_mac=_FAKE_OOB_MAC.upper(),
        tag_names=["managed-by-agent"],
    )
    h = BlueFieldHost(dmi=_DMI_FIXTURE)
    with patch.object(h, "get_netbox_server", return_value=server):
        h._drop_skeletal_tag_if_enriched()

    server.save.assert_not_called()

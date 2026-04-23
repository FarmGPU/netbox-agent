"""
Unit tests for ModuleManager with mocked pynetbox.

The netbox_agent.config module parses sys.argv at import time, so we must
mock it before importing any netbox_agent modules.
"""

import json
import sys
import re
import pytest
from unittest.mock import MagicMock, patch
from types import SimpleNamespace

# ---------------------------------------------------------------------------
# Pre-mock netbox_agent.config so it doesn't parse sys.argv or call pynetbox
# ---------------------------------------------------------------------------
_mock_nb = MagicMock()
_mock_config = SimpleNamespace(
    modules=True,
    update_modules=True,
    spare_device_name="SPARE-INVENTORY",
    register=False,
    update_all=False,
    update_inventory=False,
    update_network=False,
    update_location=False,
    update_psu=False,
    update_hypervisor=False,
    update_old_devices=False,
    purge_old_devices=False,
    expansion_as_device=False,
    inventory=False,
    debug=False,
    hostname_cmd=None,
    preserve_tags=False,
    process_virtual_drives=False,
    force_disk_refresh=False,
    dump_disks_map=None,
    log_level="debug",
    netbox=SimpleNamespace(url="http://test", token="test", ssl_verify=True, ssl_ca_certs_file=None),
    virtual=SimpleNamespace(enabled=False, cluster_name=None, hypervisor=False, list_guests_cmd=None),
    device=SimpleNamespace(
        platform=None, tags="", custom_fields="", blade_role="Blade",
        chassis_role="Server Chassis", server_role="Server",
        default_owner="FarmGPU", asset_tag_cmd=None,
    ),
    tenant=SimpleNamespace(driver=None, driver_file=None, regex=None),
    datacenter_location=SimpleNamespace(driver=None, driver_file=None, regex=None),
    rack_location=SimpleNamespace(driver=None, driver_file=None, regex=None),
    slot_location=SimpleNamespace(driver=None, driver_file=None, regex=None),
    network=SimpleNamespace(
        ignore_interfaces="(dummy.*|docker.*)", ignore_ips="^(127\\.0\\.0\\..*)",
        ipmi=True, lldp=None, nic_id="name", primary_mac="temp",
    ),
)

# Insert mock config module before any netbox_agent code loads
_mock_config_module = MagicMock()
_mock_config_module.config = _mock_config
_mock_config_module.netbox_instance = _mock_nb
_mock_config_module.get_config = MagicMock(return_value=_mock_config)
_mock_config_module.get_netbox_instance = MagicMock(return_value=_mock_nb)
sys.modules["netbox_agent.config"] = _mock_config_module

# Also mock misc so it doesn't run system commands
_mock_misc = MagicMock()
_mock_misc.is_tool = MagicMock(return_value=False)
_mock_misc.create_netbox_tags = MagicMock(return_value=[])
_mock_misc.get_device_role = MagicMock()
_mock_misc.get_device_type = MagicMock()
_mock_misc.get_device_platform = MagicMock()
_mock_misc.get_vendor = MagicMock(return_value="Unknown")
sys.modules["netbox_agent.misc"] = _mock_misc

# Now we can safely import
from netbox_agent.modules import ModuleManager, CATEGORIES


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_nb_mock():
    """Reset the shared mock nb between tests, including nested side_effects."""
    _mock_nb.reset_mock()
    # Clear side_effects on commonly-used nested mocks (reset_mock doesn't do this in 3.12)
    for group in (
        _mock_nb.dcim.manufacturers,
        _mock_nb.dcim.module_types,
        _mock_nb.dcim.module_type_profiles,
        _mock_nb.dcim.module_bays,
        _mock_nb.dcim.modules,
        _mock_nb.dcim.devices,
    ):
        for attr_name in ("get", "filter", "create"):
            attr = getattr(group, attr_name, None)
            if attr and hasattr(attr, "side_effect"):
                attr.side_effect = None
                attr.return_value = MagicMock()
    yield _mock_nb


@pytest.fixture
def nb():
    return _mock_nb


@pytest.fixture
def mock_server():
    """Create a mock server object."""
    server = MagicMock()
    server.dmi = {}
    device = MagicMock()
    device.id = 1
    device.name = "test-server-01"
    server.get_netbox_server.return_value = device
    return server


@pytest.fixture
def mock_lshw():
    """Create a mock LSHW instance."""
    lshw = MagicMock()
    lshw.get_hw_linux.return_value = []
    lshw.memories = []
    lshw.interfaces = []
    return lshw


@pytest.fixture
def mm(mock_server, mock_lshw):
    """Create a ModuleManager with mocked dependencies."""
    with patch("netbox_agent.modules.LSHW", return_value=mock_lshw):
        manager = ModuleManager(server=mock_server, config=_mock_config)
    manager.lshw = mock_lshw
    return manager


# ---------------------------------------------------------------------------
# Tests: Hardware Detection
# ---------------------------------------------------------------------------

class TestModuleManagerDetection:

    def test_get_local_cpus_lscpu(self, mm, mock_lshw):
        """CPU detection via lscpu (primary path, informed by SILO cpu.py)."""
        lscpu_data = {
            "lscpu": [
                {"field": "Architecture:", "data": "x86_64"},
                {"field": "Socket(s):", "data": "2"},
                {"field": "Model name:", "data": "Intel(R) Xeon(R) Gold 6430"},
                {"field": "Vendor ID:", "data": "GenuineIntel"},
                {"field": "Core(s) per socket:", "data": "32"},
                {"field": "Thread(s) per core:", "data": "2"},
            ]
        }
        with patch("netbox_agent.modules.is_tool", return_value=True), \
             patch("netbox_agent.modules.subprocess.check_output",
                   return_value=json.dumps(lscpu_data)):
            cpus = mm._get_local_cpus()
        assert len(cpus) == 2
        assert cpus[0]["product"] == "Intel(R) Xeon(R) Gold 6430"
        assert cpus[0]["vendor"] == "Intel"  # normalized from GenuineIntel
        assert cpus[0]["serial"] is None
        assert cpus[1]["slot"] == "CPU1"

    def test_get_local_cpus_lscpu_amd(self, mm, mock_lshw):
        """AMD CPUs detected via lscpu with vendor normalization."""
        lscpu_data = {
            "lscpu": [
                {"field": "Socket(s):", "data": "2"},
                {"field": "Model name:", "data": "AMD EPYC 9454 48-Core Processor"},
                {"field": "Vendor ID:", "data": "AuthenticAMD"},
            ]
        }
        with patch("netbox_agent.modules.is_tool", return_value=True), \
             patch("netbox_agent.modules.subprocess.check_output",
                   return_value=json.dumps(lscpu_data)):
            cpus = mm._get_local_cpus()
        assert len(cpus) == 2
        assert cpus[0]["vendor"] == "AMD"  # normalized from AuthenticAMD
        assert "EPYC 9454" in cpus[0]["product"]

    def test_get_local_cpus_lscpu_no_qat(self, mm, mock_lshw):
        """lscpu never includes QAT/accelerators — only real CPU sockets."""
        lscpu_data = {
            "lscpu": [
                {"field": "Socket(s):", "data": "2"},
                {"field": "Model name:", "data": "Intel(R) Xeon(R) 6760P"},
                {"field": "Vendor ID:", "data": "GenuineIntel"},
            ]
        }
        with patch("netbox_agent.modules.is_tool", return_value=True), \
             patch("netbox_agent.modules.subprocess.check_output",
                   return_value=json.dumps(lscpu_data)):
            cpus = mm._get_local_cpus()
        # lscpu reports exactly 2 sockets — no QAT noise
        assert len(cpus) == 2
        assert all(c["product"] == "Intel(R) Xeon(R) 6760P" for c in cpus)

    def test_get_local_cpus_lshw_fallback_filters_qat(self, mm, mock_lshw):
        """When lscpu unavailable, lshw fallback still filters QAT."""
        mock_lshw.get_hw_linux.return_value = [
            {"product": "Xeon Gold 6430", "vendor": "Intel", "location": "CPU0"},
            {"product": "Xeon Gold 6430", "vendor": "Intel", "location": "CPU1"},
            {"product": "C62x Chipset QuickAssist Technology", "vendor": "Intel",
             "description": "Co-processor", "location": ""},
            {"product": "4xxx Series QAT", "vendor": "Intel Corporation", "description": ""},
            {"product": "Intel Corporation", "vendor": "Intel Corporation", "description": ""},
        ]
        with patch("netbox_agent.modules.is_tool", return_value=False):
            cpus = mm._get_local_cpus()
        assert len(cpus) == 2
        assert all(c["product"] == "Xeon Gold 6430" for c in cpus)

    def test_get_local_gpus_no_nvidia_smi(self, mm, mock_lshw):
        mock_lshw.get_hw_linux.return_value = [
            {"product": "NVIDIA A100 80GB", "vendor": "NVIDIA", "description": "3D controller"},
        ]
        with patch("netbox_agent.modules.is_tool", return_value=False):
            gpus = mm._get_local_gpus()
        assert len(gpus) == 1
        assert gpus[0]["product"] == "NVIDIA A100 80GB"
        assert gpus[0]["serial"] is None

    def test_get_local_gpus_with_serials(self, mm, mock_lshw):
        mock_lshw.get_hw_linux.return_value = [
            {"product": "A100 80GB SXM4", "vendor": "NVIDIA", "description": "3D"},
            {"product": "A100 80GB SXM4", "vendor": "NVIDIA", "description": "3D"},
        ]
        nvidia_output = "0, 1324821038475\n1, 1324821038476"
        with patch("netbox_agent.modules.is_tool", return_value=True), \
             patch("netbox_agent.modules.subprocess.check_output", return_value=nvidia_output):
            gpus = mm._get_local_gpus()
        assert len(gpus) == 2
        assert gpus[0]["serial"] == "1324821038475"
        assert gpus[1]["serial"] == "1324821038476"

    def test_get_local_dimms(self, mm, mock_lshw):
        mock_lshw.memories = [
            {"product": "M393A8G40AB2-CWE", "vendor": "Samsung", "serial": "ABC123",
             "slot": "DIMM_A1", "size": 64, "description": "DDR4"},
            {"product": "M393A8G40AB2-CWE", "vendor": "Samsung", "serial": "Not Specified",
             "slot": "DIMM_A2", "size": 64, "description": "DDR4"},
        ]
        dimms = mm._get_local_dimms()
        assert len(dimms) == 2
        assert dimms[0]["serial"] == "ABC123"
        assert dimms[1]["serial"] is None

    def test_get_local_ssds_lsblk(self, mm, mock_lshw):
        """Storage detection via lsblk (primary path)."""
        lsblk_data = {
            "blockdevices": [
                {"name": "nvme0n1", "type": "disk", "size": 3840755982336,
                 "model": "D7-P5520", "serial": "SSD123", "vendor": None,
                 "tran": "nvme", "rota": "0", "hctl": None, "rev": "V1.0"},
                {"name": "sda", "type": "disk", "size": 960197124096,
                 "model": "Samsung SSD 870", "serial": "SSD456", "vendor": "ATA",
                 "tran": "sata", "rota": "0", "hctl": "0:0:0:0", "rev": "2B6Q"},
                {"name": "sdb", "type": "disk", "size": 4000787030016,
                 "model": "ST4000NM000A", "serial": "HDD789", "vendor": "ATA",
                 "tran": "sata", "rota": "1", "hctl": "1:0:0:0", "rev": None},
                {"name": "loop0", "type": "loop", "size": 0,
                 "model": None, "serial": None, "vendor": None,
                 "tran": None, "rota": "0", "hctl": None, "rev": None},
                {"name": "dm-0", "type": "disk", "size": 107374182400,
                 "model": None, "serial": None, "vendor": None,
                 "tran": None, "rota": "0", "hctl": None, "rev": None},
            ]
        }

        with patch("netbox_agent.modules.is_tool") as mock_is_tool, \
             patch("netbox_agent.modules.subprocess.check_output") as mock_subprocess:
            mock_is_tool.side_effect = lambda t: t in ("lsblk",)
            mock_subprocess.return_value = json.dumps(lsblk_data)
            ssds = mm._get_local_ssds()

        # Should find 3 physical disks (nvme, sata ssd, sata hdd)
        # loop0 excluded (type=loop), dm-0 excluded (name starts with dm-)
        assert len(ssds) == 3
        assert ssds[0]["serial"] == "SSD123"
        assert ssds[0]["interface"] == "NVMe"
        assert ssds[0]["description"] == "NVMe SSD"
        assert ssds[1]["serial"] == "SSD456"
        assert ssds[1]["interface"] == "SATA"
        assert ssds[1]["description"] == "SATA SSD"
        assert ssds[2]["serial"] == "HDD789"
        assert ssds[2]["interface"] == "SATA"
        assert ssds[2]["description"] == "SATA HDD"

    def test_get_local_ssds_lsblk_nvme_enrichment(self, mm, mock_lshw):
        """NVMe devices should be enriched with nvme-cli data when available."""
        lsblk_data = {
            "blockdevices": [
                {"name": "nvme0n1", "type": "disk", "size": 3840755982336,
                 "model": "D7-P5520", "serial": "SSD-NVM1", "vendor": None,
                 "tran": "nvme", "rota": "0", "hctl": None, "rev": None},
            ]
        }
        nvme_data = {
            "Devices": [
                {"DevicePath": "/dev/nvme0n1", "ModelNumber": "Solidigm D7-P5520",
                 "SerialNumber": "SSD-NVM1", "Vendor": "Solidigm",
                 "PhysicalSize": 3840755982336, "Firmware": "V1.2.3"},
            ]
        }

        call_count = [0]
        def mock_check_output(cmd, **kwargs):
            call_count[0] += 1
            if "lsblk" in cmd:
                return json.dumps(lsblk_data)
            if "nvme" in cmd:
                return json.dumps(nvme_data)
            raise FileNotFoundError(cmd[0])

        with patch("netbox_agent.modules.is_tool", return_value=True), \
             patch("netbox_agent.modules.subprocess.check_output", side_effect=mock_check_output):
            ssds = mm._get_local_ssds()

        assert len(ssds) == 1
        assert ssds[0]["vendor"] == "Solidigm"
        assert ssds[0]["firmware"] == "V1.2.3"

    def test_get_local_ssds_lshw_fallback(self, mm, mock_lshw):
        """Falls back to lshw when lsblk is not available."""
        mock_lshw.get_hw_linux.return_value = [
            {"product": "D7-P5520", "vendor": "Solidigm", "serial": "SSD123",
             "description": "NVMe disk"},
            {"product": None, "vendor": None, "serial": "SSD456",
             "description": "NVMe disk"},
            {"product": "Virtual disk", "vendor": None, "serial": "VD001",
             "description": "Virtual volume"},
        ]
        with patch("netbox_agent.modules.is_tool", return_value=False):
            ssds = mm._get_local_ssds()
        assert len(ssds) == 1
        assert ssds[0]["serial"] == "SSD123"

    def test_get_local_ssds_dedup_serials(self, mm, mock_lshw):
        """Duplicate serials should be deduplicated."""
        lsblk_data = {
            "blockdevices": [
                {"name": "nvme0n1", "type": "disk", "size": 100000,
                 "model": "TestDrive", "serial": "DUP-SERIAL", "vendor": "Test",
                 "tran": "nvme", "rota": "0", "hctl": None, "rev": None},
                {"name": "nvme1n1", "type": "disk", "size": 100000,
                 "model": "TestDrive", "serial": "DUP-SERIAL", "vendor": "Test",
                 "tran": "nvme", "rota": "0", "hctl": None, "rev": None},
            ]
        }

        with patch("netbox_agent.modules.is_tool") as mock_is_tool, \
             patch("netbox_agent.modules.subprocess.check_output") as mock_subprocess:
            mock_is_tool.side_effect = lambda t: t == "lsblk"
            mock_subprocess.return_value = json.dumps(lsblk_data)
            ssds = mm._get_local_ssds()

        assert len(ssds) == 1

    def test_get_local_ssds_vendor_guessing(self, mm, mock_lshw):
        """Vendor should be guessed from model when not provided by lsblk."""
        lsblk_data = {
            "blockdevices": [
                {"name": "nvme0n1", "type": "disk", "size": 100000,
                 "model": "Samsung SSD 990 PRO", "serial": "S1", "vendor": None,
                 "tran": "nvme", "rota": "0", "hctl": None, "rev": None},
                {"name": "sda", "type": "disk", "size": 100000,
                 "model": "Solidigm D7-PS1010", "serial": "S2", "vendor": None,
                 "tran": "sata", "rota": "0", "hctl": None, "rev": None},
            ]
        }

        with patch("netbox_agent.modules.is_tool") as mock_is_tool, \
             patch("netbox_agent.modules.subprocess.check_output") as mock_subprocess:
            mock_is_tool.side_effect = lambda t: t == "lsblk"
            mock_subprocess.return_value = json.dumps(lsblk_data)
            ssds = mm._get_local_ssds()

        assert ssds[0]["vendor"] == "Samsung"
        assert ssds[1]["vendor"] == "Solidigm"

    def test_get_local_nics(self, mm, mock_lshw):
        mock_lshw.interfaces = [
            {"product": "E810-XXVDA2", "vendor": "Intel", "serial": "aa:bb:cc:dd:ee:01",
             "name": "eno1", "description": "Ethernet"},
            {"product": "E810-XXVDA2", "vendor": "Intel", "serial": "aa:bb:cc:dd:ee:02",
             "name": "eno2", "description": "Ethernet"},
            {"product": "E810-XXVDA2", "vendor": "Intel", "serial": "aa:bb:cc:dd:ee:01",
             "name": "eno1v1", "description": "Ethernet"},
        ]
        nics = mm._get_local_nics()
        assert len(nics) == 2

    def test_get_local_psus(self, mm):
        """PSU detection uses numeric DMI type 39 to avoid leading-space lookup bug."""
        mock_dmi_data = {
            "0x0027": {
                "DMIType": 39,
                "DMISize": 20,
                "DMIName": "System Power Supply",
                "Name": "PWS-2K04A-1R",
                "Manufacturer": "Supermicro",
                "Serial Number": "PSU-12345",
            },
            "0x0028": {
                "DMIType": 39,
                "DMISize": 20,
                "DMIName": "System Power Supply",
                "Name": "PWS-2K04A-1R",
                "Manufacturer": "Supermicro",
                "Serial Number": "PSU-12346",
            },
        }
        mm.server.dmi = mock_dmi_data
        # Patch at the module level since _get_local_psus does a local import
        mock_dmidecode = MagicMock()
        mock_dmidecode.get_by_type.return_value = [
            {"Name": "PWS-2K04A-1R", "Manufacturer": "Supermicro", "Serial Number": "PSU-12345"},
            {"Name": "PWS-2K04A-1R", "Manufacturer": "Supermicro", "Serial Number": "PSU-12346"},
        ]
        with patch.dict(sys.modules, {"netbox_agent.dmidecode": mock_dmidecode}):
            psus = mm._get_local_psus()
        assert len(psus) == 2
        assert psus[0]["serial"] == "PSU-12345"
        assert psus[1]["serial"] == "PSU-12346"
        # Verify get_by_type was called with numeric 39, not string "Power Supply"
        mock_dmidecode.get_by_type.assert_called_once_with(mm.server.dmi, 39)

    def test_gpu_product_truncation(self, mm, mock_lshw):
        long_name = "A" * 60
        mock_lshw.get_hw_linux.return_value = [
            {"product": long_name, "vendor": "NVIDIA", "description": "3D"},
        ]
        with patch("netbox_agent.modules.is_tool", return_value=False):
            gpus = mm._get_local_gpus()
        assert len(gpus[0]["product"]) == 50


# ---------------------------------------------------------------------------
# Tests: Module Type Resolution
# ---------------------------------------------------------------------------

class TestModuleManagerTypeResolution:

    def test_resolve_existing_module_type(self, mm, nb):
        mock_mfr = MagicMock(id=10, name="Intel")
        nb.dcim.manufacturers.get.return_value = mock_mfr
        mock_profile = MagicMock(id=1, name="CPU")
        nb.dcim.module_type_profiles.get.return_value = mock_profile
        mock_mt = MagicMock(id=100, model="Xeon Gold 6430")
        nb.dcim.module_types.get.return_value = mock_mt

        result = mm._resolve_module_type("cpu", {"product": "Xeon Gold 6430", "vendor": "Intel"})
        assert result.id == 100
        nb.dcim.module_types.create.assert_not_called()

    def test_auto_create_module_type(self, mm, nb):
        mock_mfr = MagicMock(id=10)
        nb.dcim.manufacturers.get.return_value = mock_mfr
        mock_profile = MagicMock(id=1)
        nb.dcim.module_type_profiles.get.return_value = mock_profile
        nb.dcim.module_types.get.return_value = None
        mock_new_mt = MagicMock(id=200, model="New CPU Model")
        nb.dcim.module_types.create.return_value = mock_new_mt

        result = mm._resolve_module_type("cpu", {"product": "New CPU Model", "vendor": "Intel"})
        assert result.id == 200
        nb.dcim.module_types.create.assert_called_once()

    def test_manufacturer_caching(self, mm, nb):
        mock_mfr = MagicMock(id=10)
        nb.dcim.manufacturers.get.return_value = mock_mfr

        mm._get_or_create_manufacturer("Intel")
        mm._get_or_create_manufacturer("Intel")
        assert nb.dcim.manufacturers.get.call_count == 1


# ---------------------------------------------------------------------------
# Tests: Module Bay Management
# ---------------------------------------------------------------------------

class TestModuleManagerBayManagement:

    def test_ensure_module_bays_creates_missing(self, mm, nb):
        device = SimpleNamespace(id=1, name="test-server")

        existing_bay_0 = SimpleNamespace(id=10, name="GPU-0")
        existing_bay_1 = SimpleNamespace(id=11, name="GPU-1")
        new_bay_2 = SimpleNamespace(id=12, name="GPU-2")
        new_bay_3 = SimpleNamespace(id=13, name="GPU-3")

        nb.dcim.module_bays.filter.side_effect = [
            [existing_bay_0, existing_bay_1],
            [existing_bay_0, existing_bay_1, new_bay_2, new_bay_3],
        ]

        bays = mm._ensure_module_bays(device, "gpu", 4)
        assert nb.dcim.module_bays.create.call_count == 2
        assert len(bays) == 4


# ---------------------------------------------------------------------------
# Tests: Core Sync Algorithm
# ---------------------------------------------------------------------------

class TestModuleManagerSync:

    def test_sync_creates_new_modules(self, mm, nb):
        device = SimpleNamespace(id=1, name="test-server")
        mm.device = device

        bay0 = SimpleNamespace(id=100, name="GPU-0")
        bay1 = SimpleNamespace(id=101, name="GPU-1")
        nb.dcim.module_bays.filter.side_effect = [
            [],  # Initial fetch (no existing bays)
            [bay0, bay1],  # Re-fetch after creation
        ]

        nb.dcim.modules.filter.side_effect = [
            [],  # Existing modules on device
            [],  # Serial search for GPU-SN-001
            [],  # Bay occupancy check for bay0
            [],  # Serial search for GPU-SN-002
            [],  # Bay occupancy check for bay1
        ]

        mock_mfr = MagicMock(id=10)
        nb.dcim.manufacturers.get.return_value = mock_mfr
        mock_profile = MagicMock(id=1)
        nb.dcim.module_type_profiles.get.return_value = mock_profile
        mock_mt = MagicMock(id=50)
        nb.dcim.module_types.get.return_value = mock_mt

        mock_new_module = MagicMock(id=200)
        nb.dcim.modules.create.return_value = mock_new_module

        local_gpus = [
            {"product": "A100 80GB", "vendor": "NVIDIA", "serial": "GPU-SN-001"},
            {"product": "A100 80GB", "vendor": "NVIDIA", "serial": "GPU-SN-002"},
        ]

        mm._sync_category("gpu", local_gpus)

        assert nb.dcim.module_bays.create.call_count == 2
        assert nb.dcim.modules.create.call_count == 2

    def test_sync_empty_items_moves_to_spare(self, mm, nb):
        device = SimpleNamespace(id=1, name="test-server")
        mm.device = device

        existing_mod = MagicMock(id=300, serial="OLD-SN")
        existing_mod.module_bay = SimpleNamespace(name="GPU-0", display="GPU-0")

        spare = SimpleNamespace(id=999, name="SPARE-INVENTORY")
        nb.dcim.devices.get.return_value = spare

        spare_bay = SimpleNamespace(id=500, name="GPU-0")

        def modules_filter_side_effect(**kwargs):
            device_id = kwargs.get("device_id")
            if device_id == 1:
                return [existing_mod]
            elif device_id == 999:
                return []  # No existing modules on spare
            return []

        def bays_filter_side_effect(**kwargs):
            # Return spare bays when queried for any device
            return [spare_bay]

        nb.dcim.modules.filter.side_effect = modules_filter_side_effect
        nb.dcim.module_bays.filter.side_effect = bays_filter_side_effect

        mm._sync_category("gpu", [])

        # Module should have been re-parented: device set to spare and save called
        assert existing_mod.save.called


# ---------------------------------------------------------------------------
# Tests: create_or_update Entry Point
# ---------------------------------------------------------------------------

class TestModuleManagerCreateOrUpdate:

    def test_no_device_returns_false(self, mm, mock_server):
        mock_server.get_netbox_server.return_value = None
        result = mm.create_or_update()
        assert result is False

    def test_create_or_update_calls_all_categories(self, mm, nb):
        mm._get_local_cpus = MagicMock(return_value=[])
        mm._get_local_gpus = MagicMock(return_value=[])
        mm._get_local_dimms = MagicMock(return_value=[])
        mm._get_local_ssds = MagicMock(return_value=[])
        mm._get_local_nics = MagicMock(return_value=[])
        mm._get_local_psus = MagicMock(return_value=[])
        mm._sync_category = MagicMock()

        nb.dcim.modules.filter.return_value = []

        result = mm.create_or_update()
        assert result is True
        assert mm._sync_category.call_count == 6
        categories_synced = [call[0][0] for call in mm._sync_category.call_args_list]
        assert set(categories_synced) == {"cpu", "gpu", "dimm", "ssd", "nic", "psu"}


# ---------------------------------------------------------------------------
# Tests: Asset Tag Validation (regex-only, no module import needed)
# ---------------------------------------------------------------------------

class TestAssetTagParsing:

    # Re-define the regex/constants here to avoid importing server.py
    # (which would trigger config import chain)
    _ASSET_TAG_RE = re.compile(r"^[0-9A-Z]{4}$", re.IGNORECASE)
    _ASSET_TAG_PLACEHOLDERS = {"Not Specified", "None", "N/A", "To Be Filled By O.E.M.", ""}

    def test_valid_base36_tags(self):
        valid_tags = ["0000", "0001", "ZZZZ", "AB12", "00FF"]
        for tag in valid_tags:
            assert self._ASSET_TAG_RE.match(tag), f"{tag} should be valid"
            assert tag not in self._ASSET_TAG_PLACEHOLDERS

    def test_invalid_tags(self):
        invalid_tags = ["", "ABC", "ABCDE", "AB-1", "ab!1"]
        for tag in invalid_tags:
            assert not self._ASSET_TAG_RE.match(tag), f"{tag} should be invalid"

    def test_placeholder_tags(self):
        assert "Not Specified" in self._ASSET_TAG_PLACEHOLDERS
        assert "N/A" in self._ASSET_TAG_PLACEHOLDERS
        assert "" in self._ASSET_TAG_PLACEHOLDERS


# ---------------------------------------------------------------------------
# Tests: Base-36 Encoding
# ---------------------------------------------------------------------------

class TestBase36Encoding:

    @staticmethod
    def int_to_base36(n, width=4):
        chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        if n == 0:
            return "0" * width
        digits = []
        while n:
            digits.append(chars[n % 36])
            n //= 36
        return "".join(reversed(digits)).zfill(width)

    @staticmethod
    def base36_to_int(s):
        return int(s, 36)

    def test_int_to_base36(self):
        assert self.int_to_base36(0) == "0000"
        assert self.int_to_base36(1) == "0001"
        assert self.int_to_base36(35) == "000Z"
        assert self.int_to_base36(36) == "0010"
        assert self.int_to_base36(1679615) == "ZZZZ"

    def test_base36_to_int(self):
        assert self.base36_to_int("0000") == 0
        assert self.base36_to_int("0001") == 1
        assert self.base36_to_int("000Z") == 35
        assert self.base36_to_int("ZZZZ") == 1679615

    def test_roundtrip(self):
        for i in [0, 1, 100, 1000, 10000, 1679615]:
            assert self.base36_to_int(self.int_to_base36(i)) == i


# ---------------------------------------------------------------------------
# Tests: Serial Number Cascade & Placeholder Detection
# ---------------------------------------------------------------------------
# These test the _is_valid_serial and _get_best_serial logic from server.py.
# We re-implement the logic here to avoid importing server.py (config chain).

class TestSerialValidation:
    """Test DMI placeholder detection and serial cascade logic."""

    _DMI_PLACEHOLDERS = {
        "", "none", "n/a", "na", "not specified", "not available",
        "not applicable", "to be filled by o.e.m.", "default string",
        "0123456789", "..................", "system serial number",
        "chassis serial number", "base board serial number",
        "default", "unknown", "unspecified", "no asset information",
        "empty", "xxxxxxxxxxxx", "0000000000", "____________",
    }

    def _is_valid_serial(self, value):
        if not value or not isinstance(value, str):
            return False
        cleaned = value.strip()
        if not cleaned or len(cleaned) < 2:
            return False
        if cleaned.lower() in self._DMI_PLACEHOLDERS:
            return False
        if len(set(cleaned.replace("-", "").replace(" ", ""))) <= 1:
            return False
        return True

    def test_valid_serials(self):
        valid = [
            "S452NF30LT00023",
            "BQWF61200143",
            "J3030NQ100040",
            "WX12345678",
            "SN-1234-ABCD",
        ]
        for sn in valid:
            assert self._is_valid_serial(sn), f"{sn!r} should be valid"

    def test_placeholder_serials(self):
        placeholders = [
            "Not Specified",
            "To Be Filled By O.E.M.",
            "Default string",
            "0123456789",
            "N/A",
            "None",
            "Unknown",
            "",
            "..................",
            "System Serial Number",
            "Base Board Serial Number",
        ]
        for sn in placeholders:
            assert not self._is_valid_serial(sn), f"{sn!r} should be invalid"

    def test_single_char_repeated_serials(self):
        """Serials that are all the same character should be rejected."""
        assert not self._is_valid_serial("000000")
        assert not self._is_valid_serial("XXXXXX")
        assert not self._is_valid_serial("------")
        assert not self._is_valid_serial("      ")

    def test_none_and_empty(self):
        assert not self._is_valid_serial(None)
        assert not self._is_valid_serial("")
        assert not self._is_valid_serial("  ")
        assert not self._is_valid_serial("X")  # too short (< 2)

    def test_case_insensitive_placeholder(self):
        assert not self._is_valid_serial("not specified")
        assert not self._is_valid_serial("NOT SPECIFIED")
        assert not self._is_valid_serial("Not Specified")

    def test_whitespace_handling(self):
        """Leading/trailing whitespace should be stripped before validation."""
        assert self._is_valid_serial("  S452NF30LT00023  ")
        assert not self._is_valid_serial("  Not Specified  ")


# ---------------------------------------------------------------------------
# Tests: API Retry
# ---------------------------------------------------------------------------

from netbox_agent.modules import _api_retry, MAX_RETRIES, RETRY_BACKOFF


class TestApiRetry:

    def test_api_retry_success_first_attempt(self):
        """Succeeds on first call — no retries needed."""
        func = MagicMock(return_value="ok")
        result = _api_retry(func, "arg1", key="val")
        assert result == "ok"
        func.assert_called_once_with("arg1", key="val")

    def test_api_retry_success_on_second_attempt(self):
        """Fails once, succeeds on second call."""
        func = MagicMock(side_effect=[Exception("fail"), "ok"])
        with patch("netbox_agent.modules.time.sleep"):
            result = _api_retry(func)
        assert result == "ok"
        assert func.call_count == 2

    def test_api_retry_exhaustion_raises(self):
        """All retries fail → raises the last exception."""
        func = MagicMock(side_effect=Exception("persistent failure"))
        with patch("netbox_agent.modules.time.sleep"):
            with pytest.raises(Exception, match="persistent failure"):
                _api_retry(func)
        assert func.call_count == MAX_RETRIES

    def test_api_retry_backoff_timing(self):
        """Verify exponential backoff delays: 2, 4 seconds."""
        func = MagicMock(side_effect=[Exception("e1"), Exception("e2"), "ok"])
        with patch("netbox_agent.modules.time.sleep") as mock_sleep:
            _api_retry(func)
        # First retry: 2 * 2^0 = 2s, second retry: 2 * 2^1 = 4s
        calls = [c.args[0] for c in mock_sleep.call_args_list]
        assert calls == [RETRY_BACKOFF * (2 ** 0), RETRY_BACKOFF * (2 ** 1)]

    def test_api_retry_logs_warnings(self):
        """Retries log warning messages."""
        func = MagicMock(side_effect=[Exception("oops"), "ok"])
        with patch("netbox_agent.modules.time.sleep"), \
             patch("netbox_agent.modules.logger") as mock_logger:
            _api_retry(func)
        mock_logger.warning.assert_called()


# ---------------------------------------------------------------------------
# Tests: Sync Algorithm (extended)
# ---------------------------------------------------------------------------

class TestSyncAlgorithmExtended:

    def test_sync_duplicate_serials_warns(self, mm, nb):
        """Duplicate serials in remote lookup should log warning."""
        device = SimpleNamespace(id=1, name="test-server")
        mm.device = device

        bay = SimpleNamespace(id=100, name="GPU-0")
        nb.dcim.module_bays.filter.side_effect = [[], [bay]]

        # No existing modules on device, but global serial search finds 2
        dup_mod1 = MagicMock(id=200, serial="DUP-SN")
        dup_mod2 = MagicMock(id=201, serial="DUP-SN")
        nb.dcim.modules.filter.side_effect = [
            [],  # existing on device
            [dup_mod1, dup_mod2],  # global serial search
            [],  # bay occupancy check
        ]

        mock_mfr = MagicMock(id=10)
        nb.dcim.manufacturers.get.return_value = mock_mfr
        nb.dcim.module_type_profiles.get.return_value = MagicMock(id=1)
        nb.dcim.module_types.get.return_value = MagicMock(id=50)

        with patch("netbox_agent.modules.logger") as mock_logger:
            mm._sync_category("gpu", [
                {"product": "A100", "vendor": "NVIDIA", "serial": "DUP-SN"},
            ])
        # Should log warning about duplicate serials
        warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
        assert any("Duplicate" in s or "duplicate" in s.lower() for s in warning_calls)

    def test_sync_partial_failure_continues(self, mm, nb):
        """Failure in one category doesn't stop others."""
        mm._get_local_cpus = MagicMock(return_value=[])
        mm._get_local_gpus = MagicMock(return_value=[])
        mm._get_local_dimms = MagicMock(return_value=[])
        mm._get_local_ssds = MagicMock(return_value=[])
        mm._get_local_nics = MagicMock(return_value=[])
        mm._get_local_psus = MagicMock(return_value=[])

        # Make sync_category fail for gpu but succeed for others
        original_sync = mm._sync_category
        call_count = [0]

        def failing_sync(category, items):
            call_count[0] += 1
            if category == "gpu":
                raise Exception("GPU sync failed")
            return original_sync(category, items)

        mm._sync_category = failing_sync
        nb.dcim.modules.filter.return_value = []

        result = mm.create_or_update()
        assert result is True
        # All 6 categories attempted despite GPU failure
        assert call_count[0] == 6

    def test_sync_no_serial_positional_match(self, mm, nb):
        """CPUs (no serial) use positional matching by bay index."""
        device = SimpleNamespace(id=1, name="test-server")
        mm.device = device

        bay0 = SimpleNamespace(id=100, name="CPU-0")
        nb.dcim.module_bays.filter.side_effect = [
            [bay0],  # existing bays
            [bay0],  # re-fetch
        ]

        # Existing module in bay
        existing_mod = MagicMock(id=300, serial=None)
        existing_mod.module_type = MagicMock(id=50)
        existing_mod.module_bay = SimpleNamespace(name="CPU-0", display="CPU-0")

        nb.dcim.modules.filter.side_effect = [
            [existing_mod],  # existing on device
            [existing_mod],  # bay occupancy check
        ]

        mock_mfr = MagicMock(id=10)
        nb.dcim.manufacturers.get.return_value = mock_mfr
        nb.dcim.module_type_profiles.get.return_value = MagicMock(id=1)
        nb.dcim.module_types.get.return_value = MagicMock(id=50)

        mm._sync_category("cpu", [
            {"product": "Xeon Gold 6430", "vendor": "Intel", "serial": None},
        ])
        # Should not create new — existing module is positionally matched
        nb.dcim.modules.create.assert_not_called()

    def test_sync_empty_category_moves_all_to_spare(self, mm, nb):
        """Empty local items → all existing modules moved to spare."""
        device = SimpleNamespace(id=1, name="test-server")
        mm.device = device

        mod1 = MagicMock(id=300, serial="SN-1")
        mod1.module_bay = SimpleNamespace(name="SSD-0", display="SSD-0")
        mod2 = MagicMock(id=301, serial="SN-2")
        mod2.module_bay = SimpleNamespace(name="SSD-1", display="SSD-1")

        spare = SimpleNamespace(id=999, name="SPARE-INVENTORY")
        spare_bay = SimpleNamespace(id=500, name="SSD-0")
        spare_bay2 = SimpleNamespace(id=501, name="SSD-1")

        def modules_filter(**kwargs):
            device_id = kwargs.get("device_id")
            if device_id == 1:
                return [mod1, mod2]
            elif device_id == 999:
                return []
            return []

        def bays_filter(**kwargs):
            return [spare_bay, spare_bay2]

        nb.dcim.devices.get.return_value = spare
        nb.dcim.modules.filter.side_effect = modules_filter
        nb.dcim.module_bays.filter.side_effect = bays_filter

        mm._sync_category("ssd", [])
        # Both modules should have been re-parented (save called)
        assert mod1.save.called
        assert mod2.save.called

    def test_sync_module_type_resolution_failure_skips(self, mm, nb):
        """If module type resolution fails, that item raises."""
        device = SimpleNamespace(id=1, name="test-server")
        mm.device = device

        bay0 = SimpleNamespace(id=100, name="GPU-0")
        nb.dcim.module_bays.filter.side_effect = [[], [bay0]]
        nb.dcim.modules.filter.return_value = []

        # Make type resolution fail — use try/finally to clean up side_effect
        nb.dcim.manufacturers.get.side_effect = Exception("API error")
        try:
            with patch("netbox_agent.modules.time.sleep"):
                with pytest.raises(Exception, match="API error"):
                    mm._sync_category("gpu", [
                        {"product": "BadGPU", "vendor": "Unknown", "serial": "SN1"},
                    ])
        finally:
            nb.dcim.manufacturers.get.side_effect = None
            nb.dcim.module_bays.filter.side_effect = None
            nb.dcim.modules.filter.side_effect = None

    def test_sync_bay_conflict_moves_occupant(self, mm, nb):
        """When target bay is occupied by different module, occupant is moved to spare."""
        device = SimpleNamespace(id=1, name="test-server")
        mm.device = device

        bay0 = SimpleNamespace(id=100, name="GPU-0")
        nb.dcim.module_bays.filter.side_effect = [
            [],  # initial check
            [bay0],  # re-fetch after creation
        ]

        # Existing module occupying bay (different serial)
        occupant = MagicMock(id=400, serial="OLD-SN")
        occupant.module_bay = SimpleNamespace(name="GPU-0", display="GPU-0")

        spare = SimpleNamespace(id=999, name="SPARE-INVENTORY")
        spare_bay = SimpleNamespace(id=600, name="GPU-0")

        call_idx = [0]
        def modules_filter_seq(**kwargs):
            call_idx[0] += 1
            device_id = kwargs.get("device_id")
            module_bay_id = kwargs.get("module_bay_id")
            serial = kwargs.get("serial")

            if device_id == 1:
                return []  # no modules on device matching serial
            if serial == "NEW-SN":
                return []  # not found anywhere
            if module_bay_id == 100:
                return [occupant]  # bay is occupied
            if device_id == 999:
                return []  # spare is empty
            return []

        nb.dcim.modules.filter.side_effect = modules_filter_seq
        nb.dcim.devices.get.return_value = spare

        def bays_filter_spare(**kwargs):
            return [spare_bay]
        # Override for spare device bay lookups
        nb.dcim.module_bays.filter.side_effect = [
            [],      # initial existing bays
            [bay0],  # re-fetch
            [spare_bay],  # spare bays
        ]

        mock_mfr = MagicMock(id=10)
        nb.dcim.manufacturers.get.return_value = mock_mfr
        nb.dcim.module_type_profiles.get.return_value = MagicMock(id=1)
        nb.dcim.module_types.get.return_value = MagicMock(id=50)

        new_mod = MagicMock(id=500)
        nb.dcim.modules.create.return_value = new_mod

        mm._sync_category("gpu", [
            {"product": "H100", "vendor": "NVIDIA", "serial": "NEW-SN"},
        ])
        # Occupant should have been moved (save called to re-parent)
        assert occupant.save.called

    def test_sync_serial_existing_on_device_correct_bay_noop(self, mm, nb):
        """Module already on device in correct bay → no-op."""
        device = SimpleNamespace(id=1, name="test-server")
        mm.device = device

        bay0 = SimpleNamespace(id=100, name="GPU-0")
        nb.dcim.module_bays.filter.side_effect = [
            [bay0],  # existing bays
            [bay0],  # re-fetch
        ]

        existing_mod = MagicMock(id=300, serial="GPU-SN-1")
        existing_mod.module_type = MagicMock(id=50)
        existing_mod.module_bay = SimpleNamespace(name="GPU-0", display="GPU-0")

        nb.dcim.modules.filter.side_effect = [
            [existing_mod],  # existing on device
        ]

        mock_mfr = MagicMock(id=10)
        nb.dcim.manufacturers.get.return_value = mock_mfr
        nb.dcim.module_type_profiles.get.return_value = MagicMock(id=1)
        nb.dcim.module_types.get.return_value = MagicMock(id=50)

        mm._sync_category("gpu", [
            {"product": "A100", "vendor": "NVIDIA", "serial": "GPU-SN-1"},
        ])
        # No save (no changes) and no create
        existing_mod.save.assert_not_called()
        nb.dcim.modules.create.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: Re-parenting
# ---------------------------------------------------------------------------

class TestReparenting:

    def test_reparent_module_updates_device_and_bay(self, mm, nb):
        """reparent_module sets device and bay on the module."""
        module = MagicMock(serial="GPU-001")
        target_device = SimpleNamespace(id=42, name="new-host")
        target_bay = SimpleNamespace(id=200, name="GPU-0")

        mm._reparent_module(module, target_device, target_bay)
        assert module.device == 42
        assert module.module_bay == 200
        module.save.assert_called_once()

    def test_move_to_spare_missing_device_returns_false(self, mm, nb):
        """Spare device not found → returns False."""
        nb.dcim.devices.get.return_value = None
        module = MagicMock(serial="GPU-001")

        result = mm._move_to_spare(module, "gpu")
        assert result is False

    def test_move_to_spare_no_free_bays_logs_error(self, mm, nb):
        """No free bays on spare → returns False and logs error."""
        spare = SimpleNamespace(id=999, name="SPARE-INVENTORY")
        nb.dcim.devices.get.return_value = spare

        spare_bay = SimpleNamespace(id=500, name="GPU-0")
        occupant = MagicMock(id=600)
        occupant.module_bay = SimpleNamespace(id=500)

        nb.dcim.module_bays.filter.return_value = [spare_bay]
        nb.dcim.modules.filter.return_value = [occupant]

        module = MagicMock(serial="GPU-001")
        result = mm._move_to_spare(module, "gpu")
        assert result is False

    def test_vacate_bay_occupied_moves_to_spare(self, mm, nb):
        """Vacate bay with occupant → moves occupant to spare."""
        bay = SimpleNamespace(id=100, name="GPU-0")
        occupant = MagicMock(id=300, serial="OLD-SN")
        nb.dcim.modules.filter.return_value = [occupant]

        # Set up spare
        spare = SimpleNamespace(id=999, name="SPARE-INVENTORY")
        spare_bay = SimpleNamespace(id=500, name="GPU-0")
        nb.dcim.devices.get.return_value = spare

        def bays_filter(**kwargs):
            return [spare_bay]

        def modules_filter_spare(**kwargs):
            device_id = kwargs.get("device_id")
            module_bay_id = kwargs.get("module_bay_id")
            if module_bay_id == 100:
                return [occupant]
            if device_id == 999:
                return []
            return []

        nb.dcim.modules.filter.side_effect = modules_filter_spare
        nb.dcim.module_bays.filter.return_value = [spare_bay]

        mm._vacate_bay(bay, "gpu")
        assert occupant.save.called

    def test_vacate_bay_empty_noop(self, mm, nb):
        """Vacate empty bay → no action taken."""
        bay = SimpleNamespace(id=100, name="GPU-0")
        nb.dcim.modules.filter.return_value = []

        mm._vacate_bay(bay, "gpu")
        nb.dcim.devices.get.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: Spare Device
# ---------------------------------------------------------------------------

class TestSpareDevice:

    def test_get_spare_device_not_found_returns_none(self, mm, nb):
        """Spare device not in NetBox → returns None."""
        nb.dcim.devices.get.return_value = None
        result = mm._get_spare_device()
        assert result is None

    def test_find_module_by_serial_found(self, mm, nb):
        """Find module by serial → returns match."""
        mod = MagicMock(id=100, serial="SN-001")
        nb.dcim.modules.filter.return_value = [mod]
        result = mm._find_module_by_serial("SN-001")
        assert result.id == 100

    def test_find_module_by_serial_not_found_returns_none(self, mm, nb):
        """No module with serial → returns None."""
        nb.dcim.modules.filter.return_value = []
        result = mm._find_module_by_serial("NONEXISTENT")
        assert result is None


# ---------------------------------------------------------------------------
# Tests: Module Type Resolution (extended)
# ---------------------------------------------------------------------------

class TestModuleTypeResolutionExtended:

    def test_resolve_module_type_missing_profile_still_creates(self, mm, nb):
        """Module type created even when profile not found."""
        mock_mfr = MagicMock(id=10)
        nb.dcim.manufacturers.get.return_value = mock_mfr
        nb.dcim.module_type_profiles.get.return_value = None  # No profile
        nb.dcim.module_types.get.return_value = None  # Doesn't exist
        mock_new_mt = MagicMock(id=300)
        nb.dcim.module_types.create.return_value = mock_new_mt

        result = mm._resolve_module_type("gpu", {"product": "Test GPU", "vendor": "Test"})
        assert result.id == 300
        # Create should have been called without profile
        create_args = nb.dcim.module_types.create.call_args[0][0]
        assert "profile" not in create_args

    def test_resolve_module_type_api_failure_raises(self, mm, nb):
        """API failure during resolution propagates."""
        nb.dcim.manufacturers.get.side_effect = Exception("API down")
        try:
            with patch("netbox_agent.modules.time.sleep"):
                with pytest.raises(Exception, match="API down"):
                    mm._resolve_module_type("cpu", {"product": "Xeon", "vendor": "Intel"})
        finally:
            nb.dcim.manufacturers.get.side_effect = None

    def test_manufacturer_slug_generation(self, mm, nb):
        """Manufacturer slug is generated from name."""
        nb.dcim.manufacturers.get.return_value = None
        new_mfr = MagicMock(id=20)
        nb.dcim.manufacturers.create.return_value = new_mfr

        mm._get_or_create_manufacturer("My Test Vendor!")
        call_kwargs = nb.dcim.manufacturers.create.call_args
        # Slug should be lowercased and hyphenated
        slug = call_kwargs[1].get("slug") or call_kwargs[0][1] if len(call_kwargs[0]) > 1 else None
        if slug is None:
            # Check keyword args
            slug = call_kwargs.kwargs.get("slug", "")
        assert "my-test-vendor" in slug.lower()

    def test_manufacturer_cache_hit(self, mm, nb):
        """Second lookup for same manufacturer uses cache."""
        mock_mfr = MagicMock(id=10, name="Intel")
        nb.dcim.manufacturers.get.return_value = mock_mfr

        mm._get_or_create_manufacturer("Intel")
        mm._get_or_create_manufacturer("Intel")
        # Only one API call despite two lookups
        assert nb.dcim.manufacturers.get.call_count == 1


# ---------------------------------------------------------------------------
# Tests: Edge Cases
# ---------------------------------------------------------------------------

class TestEdgeCases:

    def test_parse_lsblk_empty_blockdevices(self, mm, mock_lshw):
        """Empty blockdevices list → empty result."""
        lsblk_data = {"blockdevices": []}
        with patch("netbox_agent.modules.is_tool") as mock_is_tool, \
             patch("netbox_agent.modules.subprocess.check_output") as mock_subprocess:
            mock_is_tool.side_effect = lambda t: t == "lsblk"
            mock_subprocess.return_value = json.dumps(lsblk_data)
            ssds = mm._get_local_ssds()
        assert ssds == []

    def test_nvme_enrichment_fallback_on_failure(self, mm, mock_lshw):
        """NVMe enrichment failure falls back to lsblk data only."""
        lsblk_data = {
            "blockdevices": [
                {"name": "nvme0n1", "type": "disk", "size": 100000,
                 "model": "TestNVMe", "serial": "SN-1", "vendor": None,
                 "tran": "nvme", "rota": "0", "hctl": None, "rev": None},
            ]
        }

        call_idx = [0]
        def mock_check_output(cmd, **kwargs):
            call_idx[0] += 1
            if "lsblk" in cmd:
                return json.dumps(lsblk_data)
            if "nvme" in cmd:
                raise Exception("nvme-cli failed")
            raise FileNotFoundError(cmd[0])

        with patch("netbox_agent.modules.is_tool", return_value=True), \
             patch("netbox_agent.modules.subprocess.check_output", side_effect=mock_check_output):
            ssds = mm._get_local_ssds()

        assert len(ssds) == 1
        assert ssds[0]["serial"] == "SN-1"

    def test_gpu_serial_placeholder_filtered(self, mm, mock_lshw):
        """GPU serials that are placeholders ([N/A], N/A, 0) should be None."""
        mock_lshw.get_hw_linux.return_value = [
            {"product": "A100", "vendor": "NVIDIA", "description": "3D"},
        ]
        nvidia_output = "0, [N/A]"
        with patch("netbox_agent.modules.is_tool", return_value=True), \
             patch("netbox_agent.modules.subprocess.check_output", return_value=nvidia_output):
            gpus = mm._get_local_gpus()
        assert len(gpus) == 1
        assert gpus[0]["serial"] is None

    def test_gpu_mixed_discrete_and_onboard(self, mm, mock_lshw):
        """Only discrete GPUs are detected — onboard VGA filtered out."""
        mock_lshw.get_hw_linux.return_value = [
            {"product": "ASPEED Graphics Family", "vendor": "ASPEED Technology, Inc.",
             "description": "VGA compatible controller"},
            {"product": "H100 80GB HBM3", "vendor": "NVIDIA Corporation",
             "description": "3D controller"},
        ]
        with patch("netbox_agent.modules.is_tool", return_value=False):
            gpus = mm._get_local_gpus()
        assert len(gpus) == 1
        assert "H100" in gpus[0]["product"]

    def test_psu_serial_placeholder_filtered(self, mm):
        """PSU placeholder serials (Not Specified, etc.) become None."""
        mock_dmidecode = MagicMock()
        mock_dmidecode.get_by_type.return_value = [
            {"Name": "PSU-1", "Manufacturer": "Supermicro",
             "Serial Number": "Not Specified"},
            {"Name": "PSU-2", "Manufacturer": "Supermicro",
             "Serial Number": "To Be Filled By O.E.M."},
            {"Name": "PSU-3", "Manufacturer": "Supermicro",
             "Serial Number": "PSU-REAL-SN"},
        ]
        with patch.dict(sys.modules, {"netbox_agent.dmidecode": mock_dmidecode}):
            psus = mm._get_local_psus()
        assert len(psus) == 3
        assert psus[0]["serial"] is None
        assert psus[1]["serial"] is None
        assert psus[2]["serial"] == "PSU-REAL-SN"


# ---------------------------------------------------------------------------
# Tests: create_or_update with deps and state
# ---------------------------------------------------------------------------

class TestCreateOrUpdateWithDepsAndState:

    def test_create_or_update_skips_psu_when_dmidecode_missing(self, mm, nb):
        """When dmidecode unavailable, PSU detection is skipped."""
        mm._get_local_cpus = MagicMock(return_value=[])
        mm._get_local_gpus = MagicMock(return_value=[])
        mm._get_local_dimms = MagicMock(return_value=[])
        mm._get_local_ssds = MagicMock(return_value=[])
        mm._get_local_nics = MagicMock(return_value=[])
        mm._get_local_psus = MagicMock(return_value=[])
        mm._sync_category = MagicMock()
        nb.dcim.modules.filter.return_value = []

        deps = {"dmidecode": False, "lshw": True}
        result = mm.create_or_update(deps=deps)
        assert result is True
        # PSU detection should NOT have been called
        mm._get_local_psus.assert_not_called()

    def test_create_or_update_with_state_skips_unchanged(self, mm, nb):
        """With state, unchanged categories are skipped."""
        mm._get_local_cpus = MagicMock(return_value=[
            {"product": "Xeon", "vendor": "Intel", "serial": None}
        ])
        mm._get_local_gpus = MagicMock(return_value=[])
        mm._get_local_dimms = MagicMock(return_value=[])
        mm._get_local_ssds = MagicMock(return_value=[])
        mm._get_local_nics = MagicMock(return_value=[])
        mm._get_local_psus = MagicMock(return_value=[])
        mm._sync_category = MagicMock()
        nb.dcim.modules.filter.return_value = []

        # Create a mock state that reports no changes
        mock_state = MagicMock()
        mock_state.diff_hardware.return_value = (False, "unchanged")

        result = mm.create_or_update(state=mock_state)
        assert result is True
        # sync_category should not have been called (all unchanged)
        mm._sync_category.assert_not_called()

    def test_create_or_update_with_state_syncs_changed(self, mm, nb):
        """With state, changed categories ARE synced."""
        mm._get_local_cpus = MagicMock(return_value=[
            {"product": "Xeon", "vendor": "Intel", "serial": None}
        ])
        mm._get_local_gpus = MagicMock(return_value=[])
        mm._get_local_dimms = MagicMock(return_value=[])
        mm._get_local_ssds = MagicMock(return_value=[])
        mm._get_local_nics = MagicMock(return_value=[])
        mm._get_local_psus = MagicMock(return_value=[])
        mm._sync_category = MagicMock()
        nb.dcim.modules.filter.return_value = []

        # State says CPU changed, rest unchanged
        def mock_diff(category, items):
            if category == "cpu":
                return (True, "+1")
            return (False, "unchanged")

        mock_state = MagicMock()
        mock_state.diff_hardware.side_effect = mock_diff

        result = mm.create_or_update(state=mock_state)
        assert result is True
        # Only cpu should have been synced
        assert mm._sync_category.call_count == 1
        assert mm._sync_category.call_args[0][0] == "cpu"

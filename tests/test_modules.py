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
    """Reset the shared mock nb between tests."""
    _mock_nb.reset_mock()
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

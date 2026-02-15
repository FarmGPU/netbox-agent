"""
Integration tests using real hardware fixture data.

These tests load JSON fixture files (captured from real servers by
scripts/fixtures/collect_hardware_fixture.py) and validate that the
detection logic in ModuleManager and LSHW correctly parses the data.

Fixture files in tests/fixtures/ contain raw lshw, lsblk, nvme, dmidecode
output plus an "expected" section with expected detection counts.
"""

import json
import os
import sys
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
from types import SimpleNamespace

# ---------------------------------------------------------------------------
# Pre-mock netbox_agent.config (same approach as test_modules.py)
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

_mock_config_module = MagicMock()
_mock_config_module.config = _mock_config
_mock_config_module.netbox_instance = _mock_nb
_mock_config_module.get_config = MagicMock(return_value=_mock_config)
_mock_config_module.get_netbox_instance = MagicMock(return_value=_mock_nb)

if "netbox_agent.config" not in sys.modules or isinstance(sys.modules["netbox_agent.config"], MagicMock):
    sys.modules["netbox_agent.config"] = _mock_config_module

_mock_misc = MagicMock()
_mock_misc.is_tool = MagicMock(return_value=False)
_mock_misc.create_netbox_tags = MagicMock(return_value=[])
_mock_misc.get_device_role = MagicMock()
_mock_misc.get_device_type = MagicMock()
_mock_misc.get_device_platform = MagicMock()
_mock_misc.get_vendor = MagicMock(return_value="Unknown")

if "netbox_agent.misc" not in sys.modules or isinstance(sys.modules["netbox_agent.misc"], MagicMock):
    sys.modules["netbox_agent.misc"] = _mock_misc

from netbox_agent.modules import ModuleManager
from netbox_agent.lshw import LSHW


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name):
    """Load a fixture JSON file from tests/fixtures/."""
    path = FIXTURES_DIR / name
    if not path.exists():
        pytest.skip(f"Fixture not found: {path}")
    with open(path) as f:
        return json.load(f)


def _build_lshw_from_fixture(fixture):
    """Build a real LSHW object from fixture lshw data (bypassing subprocess)."""
    lshw_data = fixture.get("lshw", {})
    if not lshw_data.get("available") or "data" not in lshw_data:
        return None

    hw_data = lshw_data["data"]
    # If the data is a list (lshw >= 02.18), unwrap
    if isinstance(hw_data, list):
        hw_data = hw_data[0]

    # Construct LSHW by patching subprocess and is_tool
    with patch("netbox_agent.lshw.subprocess") as mock_sub, \
         patch("netbox_agent.lshw.is_tool", return_value=True):
        mock_sub.getoutput.return_value = json.dumps(hw_data)
        lshw = LSHW()
    return lshw


def _build_module_manager(fixture, lshw_instance):
    """Build a ModuleManager with the fixture's lshw data injected."""
    server = MagicMock()
    server.dmi = {}
    device = MagicMock()
    device.id = 1
    device.name = fixture.get("system", {}).get("hostname", "fixture-server")
    server.get_netbox_server.return_value = device

    with patch("netbox_agent.modules.LSHW", return_value=lshw_instance):
        mm = ModuleManager(server=server, config=_mock_config)
    mm.lshw = lshw_instance
    return mm


# ---------------------------------------------------------------------------
# Parametrized fixture discovery
# ---------------------------------------------------------------------------

def _discover_fixtures():
    """Find all fixture JSON files in tests/fixtures/."""
    fixtures = []
    if FIXTURES_DIR.exists():
        for f in sorted(FIXTURES_DIR.glob("*.json")):
            fixtures.append(f.name)
    return fixtures


fixture_files = _discover_fixtures()


# ---------------------------------------------------------------------------
# Tests: LSHW Parsing with Real Data
# ---------------------------------------------------------------------------

class TestLSHWFixtureParsing:
    """Test that LSHW correctly parses hardware from fixture data."""

    @pytest.mark.parametrize("fixture_name", fixture_files)
    def test_lshw_parses_fixture(self, fixture_name):
        """LSHW should successfully parse fixture data without errors."""
        fixture = _load_fixture(fixture_name)
        lshw = _build_lshw_from_fixture(fixture)
        if lshw is None:
            pytest.skip("No lshw data in fixture")

        # Basic sanity: lshw object should be created successfully
        assert lshw is not None
        assert lshw.vendor is not None
        assert lshw.product is not None

    @pytest.mark.parametrize("fixture_name", fixture_files)
    def test_cpu_count_matches_expected(self, fixture_name):
        """CPU count from lshw should match expected (QAT filtered)."""
        fixture = _load_fixture(fixture_name)
        expected = fixture.get("expected", {})
        if "cpus" not in expected:
            pytest.skip("No expected cpu count in fixture")

        lshw = _build_lshw_from_fixture(fixture)
        if lshw is None:
            pytest.skip("No lshw data in fixture")

        mm = _build_module_manager(fixture, lshw)
        cpus = mm._get_local_cpus()
        assert len(cpus) == expected["cpus"], \
            f"Expected {expected['cpus']} CPUs, got {len(cpus)}: {[c['product'] for c in cpus]}"

    @pytest.mark.parametrize("fixture_name", fixture_files)
    def test_gpu_count_matches_expected(self, fixture_name):
        """GPU count from lshw should match expected (BMC VGA filtered)."""
        fixture = _load_fixture(fixture_name)
        expected = fixture.get("expected", {})
        if "gpus" not in expected:
            pytest.skip("No expected gpu count in fixture")

        lshw = _build_lshw_from_fixture(fixture)
        if lshw is None:
            pytest.skip("No lshw data in fixture")

        mm = _build_module_manager(fixture, lshw)
        # Disable nvidia-smi in test
        with patch("netbox_agent.modules.is_tool", return_value=False):
            gpus = mm._get_local_gpus()
        assert len(gpus) == expected["gpus"], \
            f"Expected {expected['gpus']} GPUs, got {len(gpus)}: {[g['product'] for g in gpus]}"

    @pytest.mark.parametrize("fixture_name", fixture_files)
    def test_dimm_count_matches_expected(self, fixture_name):
        """DIMM count from lshw should match expected."""
        fixture = _load_fixture(fixture_name)
        expected = fixture.get("expected", {})
        if "dimms" not in expected:
            pytest.skip("No expected dimm count in fixture")

        lshw = _build_lshw_from_fixture(fixture)
        if lshw is None:
            pytest.skip("No lshw data in fixture")

        mm = _build_module_manager(fixture, lshw)
        dimms = mm._get_local_dimms()
        assert len(dimms) == expected["dimms"], \
            f"Expected {expected['dimms']} DIMMs, got {len(dimms)}"

    @pytest.mark.parametrize("fixture_name", fixture_files)
    def test_nic_count_matches_expected(self, fixture_name):
        """NIC count from lshw should match expected."""
        fixture = _load_fixture(fixture_name)
        expected = fixture.get("expected", {})
        if "nics" not in expected:
            pytest.skip("No expected nic count in fixture")

        lshw = _build_lshw_from_fixture(fixture)
        if lshw is None:
            pytest.skip("No lshw data in fixture")

        mm = _build_module_manager(fixture, lshw)
        nics = mm._get_local_nics()
        assert len(nics) == expected["nics"], \
            f"Expected {expected['nics']} NICs, got {len(nics)}: {[n['product'] for n in nics]}"


# ---------------------------------------------------------------------------
# Tests: Storage Detection with lsblk Fixture Data
# ---------------------------------------------------------------------------

class TestStorageFixtureDetection:
    """Test lsblk-based storage detection using fixture data."""

    @pytest.mark.parametrize("fixture_name", fixture_files)
    def test_storage_count_matches_expected(self, fixture_name):
        """Storage count from lsblk should match expected."""
        fixture = _load_fixture(fixture_name)
        expected = fixture.get("expected", {})
        if "storage" not in expected:
            pytest.skip("No expected storage count in fixture")

        lsblk_data = fixture.get("lsblk", {})
        nvme_data = fixture.get("nvme", {})
        if not lsblk_data.get("available"):
            pytest.skip("No lsblk data in fixture")

        lshw = _build_lshw_from_fixture(fixture)
        if lshw is None:
            pytest.skip("No lshw data in fixture")

        mm = _build_module_manager(fixture, lshw)

        def mock_check_output(cmd, **kwargs):
            if isinstance(cmd, list) and "lsblk" in cmd:
                return json.dumps(lsblk_data["data"])
            if isinstance(cmd, list) and "nvme" in cmd:
                if nvme_data.get("available") and "data" in nvme_data:
                    return json.dumps(nvme_data["data"])
                raise FileNotFoundError("nvme")
            raise FileNotFoundError(str(cmd))

        with patch("netbox_agent.modules.is_tool", return_value=True), \
             patch("netbox_agent.modules.subprocess.check_output", side_effect=mock_check_output):
            storage = mm._get_local_ssds()

        assert len(storage) == expected["storage"], \
            f"Expected {expected['storage']} storage devices, got {len(storage)}: {[s['product'] for s in storage]}"

    @pytest.mark.parametrize("fixture_name", fixture_files)
    def test_storage_interfaces_match_expected(self, fixture_name):
        """Storage interfaces should match expected (NVMe, SATA, SAS, etc.)."""
        fixture = _load_fixture(fixture_name)
        expected = fixture.get("expected", {})
        if "storage_interfaces" not in expected:
            pytest.skip("No expected storage_interfaces in fixture")

        lsblk_data = fixture.get("lsblk", {})
        nvme_data = fixture.get("nvme", {})
        if not lsblk_data.get("available"):
            pytest.skip("No lsblk data in fixture")

        lshw = _build_lshw_from_fixture(fixture)
        if lshw is None:
            pytest.skip("No lshw data in fixture")

        mm = _build_module_manager(fixture, lshw)

        def mock_check_output(cmd, **kwargs):
            if isinstance(cmd, list) and "lsblk" in cmd:
                return json.dumps(lsblk_data["data"])
            if isinstance(cmd, list) and "nvme" in cmd:
                if nvme_data.get("available") and "data" in nvme_data:
                    return json.dumps(nvme_data["data"])
                raise FileNotFoundError("nvme")
            raise FileNotFoundError(str(cmd))

        with patch("netbox_agent.modules.is_tool", return_value=True), \
             patch("netbox_agent.modules.subprocess.check_output", side_effect=mock_check_output):
            storage = mm._get_local_ssds()

        interfaces = [s.get("interface") for s in storage]
        assert interfaces == expected["storage_interfaces"], \
            f"Expected interfaces {expected['storage_interfaces']}, got {interfaces}"

    @pytest.mark.parametrize("fixture_name", fixture_files)
    def test_nvme_enrichment_adds_vendor(self, fixture_name):
        """NVMe devices should have vendor from nvme-cli when lsblk doesn't provide one."""
        fixture = _load_fixture(fixture_name)
        lsblk_data = fixture.get("lsblk", {})
        nvme_data = fixture.get("nvme", {})
        if not lsblk_data.get("available") or not nvme_data.get("available"):
            pytest.skip("Need both lsblk and nvme data")

        lshw = _build_lshw_from_fixture(fixture)
        if lshw is None:
            pytest.skip("No lshw data in fixture")

        mm = _build_module_manager(fixture, lshw)

        def mock_check_output(cmd, **kwargs):
            if isinstance(cmd, list) and "lsblk" in cmd:
                return json.dumps(lsblk_data["data"])
            if isinstance(cmd, list) and "nvme" in cmd:
                return json.dumps(nvme_data["data"])
            raise FileNotFoundError(str(cmd))

        with patch("netbox_agent.modules.is_tool", return_value=True), \
             patch("netbox_agent.modules.subprocess.check_output", side_effect=mock_check_output):
            storage = mm._get_local_ssds()

        # Check NVMe devices have vendor populated
        for item in storage:
            if item.get("interface") == "NVMe":
                assert item["vendor"] != "Unknown", \
                    f"NVMe device {item['product']} should have vendor from nvme-cli"


# ---------------------------------------------------------------------------
# Tests: GPU Serial Detection with nvidia-smi Fixture Data
# ---------------------------------------------------------------------------

class TestGPUSerialFixtureDetection:

    @pytest.mark.parametrize("fixture_name", fixture_files)
    def test_gpu_serials_match_expected(self, fixture_name):
        """GPU serials from nvidia-smi should match expected."""
        fixture = _load_fixture(fixture_name)
        expected = fixture.get("expected", {})
        nvidia = fixture.get("nvidia", {})

        if "gpu_serials" not in expected:
            pytest.skip("No expected gpu_serials in fixture")
        if not nvidia.get("available"):
            pytest.skip("No nvidia-smi data in fixture")

        lshw = _build_lshw_from_fixture(fixture)
        if lshw is None:
            pytest.skip("No lshw data in fixture")

        mm = _build_module_manager(fixture, lshw)

        def mock_is_tool(name):
            return name == "nvidia-smi"

        with patch("netbox_agent.modules.is_tool", side_effect=mock_is_tool), \
             patch("netbox_agent.modules.subprocess.check_output",
                   return_value=nvidia.get("query_csv", "")):
            gpus = mm._get_local_gpus()

        serials = [g["serial"] for g in gpus if g.get("serial")]
        assert serials == expected["gpu_serials"], \
            f"Expected GPU serials {expected['gpu_serials']}, got {serials}"


# ---------------------------------------------------------------------------
# Tests: Interface Detection Unit Tests
# ---------------------------------------------------------------------------

class TestInterfaceDetection:
    """Test _detect_storage_interface and _build_storage_description."""

    @pytest.fixture
    def mm(self):
        """Build a minimal ModuleManager for calling static-like methods."""
        lshw = MagicMock()
        lshw.get_hw_linux.return_value = []
        lshw.memories = []
        lshw.interfaces = []
        server = MagicMock()
        server.dmi = {}
        device = MagicMock()
        device.id = 1
        device.name = "test"
        server.get_netbox_server.return_value = device
        with patch("netbox_agent.modules.LSHW", return_value=lshw):
            return ModuleManager(server=server, config=_mock_config)

    @pytest.mark.parametrize("tran,name,expected", [
        ("nvme", "nvme0n1", "NVMe"),
        ("sata", "sda", "SATA"),
        ("sas", "sdb", "SAS"),
        ("usb", "sdc", "USB"),
        ("ata", "sdd", "SATA"),
        ("fc", "sde", "FC"),
        ("", "nvme0n1", "NVMe"),
        ("", "sda", "SATA"),
        ("", "hda", "IDE"),
        (None, "xvda", None),
    ])
    def test_detect_storage_interface(self, mm, tran, name, expected):
        result = mm._detect_storage_interface(tran or "", name)
        assert result == expected, f"tran={tran!r}, name={name!r}: expected {expected!r}, got {result!r}"

    @pytest.mark.parametrize("interface,rota,expected", [
        ("NVMe", "0", "NVMe SSD"),
        ("NVMe", 0, "NVMe SSD"),
        ("SATA", "0", "SATA SSD"),
        ("SATA", "1", "SATA HDD"),
        ("SATA", 1, "SATA HDD"),
        ("SAS", "1", "SAS HDD"),
        ("SAS", "0", "SAS SSD"),
        (None, None, "disk"),
        ("NVMe", None, "NVMe disk"),
    ])
    def test_build_storage_description(self, mm, interface, rota, expected):
        result = mm._build_storage_description(interface, rota)
        assert result == expected, f"interface={interface!r}, rota={rota!r}: expected {expected!r}, got {result!r}"


# ---------------------------------------------------------------------------
# Tests: Vendor Guessing
# ---------------------------------------------------------------------------

class TestVendorGuessing:

    @pytest.fixture
    def mm(self):
        lshw = MagicMock()
        lshw.get_hw_linux.return_value = []
        lshw.memories = []
        lshw.interfaces = []
        server = MagicMock()
        server.dmi = {}
        device = MagicMock()
        device.id = 1
        device.name = "test"
        server.get_netbox_server.return_value = device
        with patch("netbox_agent.modules.LSHW", return_value=lshw):
            return ModuleManager(server=server, config=_mock_config)

    @pytest.mark.parametrize("model,expected_vendor", [
        ("Samsung SSD 990 PRO 2TB", "Samsung"),
        ("Solidigm D7-P5520", "Solidigm"),
        ("Intel SSDPE2KX040T8", "Intel"),
        ("Micron_5300_MTFDDAK960TDS", "Micron"),
        ("WDC WD4003FFBX-68MU3N0", "Western Digital"),
        ("ST4000NM000A-2HZ100", "Seagate"),
        ("KIOXIA KCM61RUL3T84", "Kioxia"),
        ("HGST HUS726040ALE614", "HGST"),
        ("Hitachi HDS723020BLA642", "Hitachi"),
        ("UNKNOWN-MODEL-XYZ", None),
    ])
    def test_guess_vendor(self, mm, model, expected_vendor):
        result = mm._guess_vendor(model)
        assert result == expected_vendor, f"model={model!r}: expected {expected_vendor!r}, got {result!r}"

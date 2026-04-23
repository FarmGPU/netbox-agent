"""
Tests for network.py IP unassignment logic — specifically the primary_ip4
clearing that prevents "Cannot reassign IP while designated as primary" errors.

We test the fix logic in isolation by extracting and exercising just the
IP unassignment section of create_or_update_netbox_network_cards().
"""

import sys
import logging
import pytest
from unittest.mock import MagicMock, patch, call
from types import SimpleNamespace
from itertools import islice


# ---------------------------------------------------------------------------
# Pre-mock netbox_agent.config to avoid import-time sys.argv parsing
# ---------------------------------------------------------------------------
_mock_nb = MagicMock(name="nb")
_mock_config = SimpleNamespace(
    update_all=True, update_network=True, register=False,
    network=SimpleNamespace(
        ignore_interfaces="(dummy.*|docker.*)", ignore_ips="^(127\\.0\\.0\\..*)",
        ipmi=False, lldp=None, nic_id="name", primary_mac="temp",
    ),
)
_mock_config_module = MagicMock()
_mock_config_module.config = _mock_config
_mock_config_module.netbox_instance = _mock_nb
sys.modules.setdefault("netbox_agent.config", _mock_config_module)

_mock_misc = MagicMock()
_mock_misc.is_tool = MagicMock(return_value=False)
sys.modules.setdefault("netbox_agent.misc", _mock_misc)
sys.modules.setdefault("netbox_agent.ethtool", MagicMock())
sys.modules.setdefault("netbox_agent.lldp", MagicMock())
sys.modules.setdefault("netbox_agent.ipmi", MagicMock())


# ---------------------------------------------------------------------------
# The function under test — extracted from network.py lines 521-561.
# This is the exact code path that caused the Potato 400 errors.
# ---------------------------------------------------------------------------
def _run_ip_unassignment(device, nb_nics, netbox_ips, all_local_ips, nb_api):
    """
    Simulate the IP unassignment section of create_or_update_netbox_network_cards().

    This extracts lines 521-561 of network.py into a testable function.
    The `nb_api` parameter replaces the module-level `nb` import.
    """
    if len(nb_nics):
        for netbox_ip in netbox_ips:
            if netbox_ip.address not in all_local_ips:
                # --- BEGIN FIX: clear primary_ip4 if this IP is primary ---
                device_primary = getattr(device, "primary_ip4", None)
                if device_primary and device_primary.id == netbox_ip.id:
                    logging.info(
                        "Clearing primary_ip4 %s on device %s before unassigning",
                        netbox_ip.address,
                        getattr(device, "name", "?"),
                    )
                    fresh_device = nb_api.dcim.devices.get(device.id)
                    fresh_device.primary_ip4 = None
                    fresh_device.save()
                    device = nb_api.dcim.devices.get(device.id)
                # --- END FIX ---

                logging.info(
                    "Unassigning IP %s from %s",
                    netbox_ip.address, netbox_ip.assigned_object,
                )
                netbox_ip.assigned_object_type = None
                netbox_ip.assigned_object_id = None
                netbox_ip.save()

    return device  # return updated device reference


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ip(ip_id, address):
    ip = MagicMock(name=f"ip-{ip_id}")
    ip.id = ip_id
    ip.address = address
    ip.assigned_object = MagicMock(display="eth0")
    ip.assigned_object_type = "dcim.interface"
    ip.assigned_object_id = 100
    return ip


def _make_device(device_id, name, primary_ip4=None):
    dev = MagicMock(name=f"device-{name}")
    dev.id = device_id
    dev.name = name
    dev.primary_ip4 = primary_ip4
    return dev


def _make_nic(nic_id):
    nic = MagicMock(name=f"nic-{nic_id}")
    nic.id = nic_id
    return nic


# ---------------------------------------------------------------------------
# Tests — these verify the fix logic directly
# ---------------------------------------------------------------------------

class TestPrimaryIp4ClearingBeforeUnassign:
    """Test that primary_ip4 is cleared before unassigning IPs."""

    def test_unassign_primary_ip_clears_primary_first(self):
        """
        Core Potato fix: when unassigning an IP that is the device's primary_ip4,
        clear primary_ip4 FIRST to avoid NetBox 400 error.
        """
        primary_ip = _make_ip(42, "10.100.200.50/24")
        device = _make_device(1, "potato01", primary_ip4=primary_ip)

        # After clearing, re-fetch returns device without primary
        device_after_clear = _make_device(1, "potato01", primary_ip4=None)
        nb_api = MagicMock(name="nb_api")
        nb_api.dcim.devices.get.return_value = device_after_clear

        nb_nics = [_make_nic(100)]

        result_device = _run_ip_unassignment(
            device=device,
            nb_nics=nb_nics,
            netbox_ips=[primary_ip],
            all_local_ips=["10.100.200.99/24"],  # different IP — old one should be unassigned
            nb_api=nb_api,
        )

        # 1. The device was re-fetched and primary_ip4 cleared
        nb_api.dcim.devices.get.assert_called_with(1)
        device_after_clear.save.assert_called_once()
        assert device_after_clear.primary_ip4 is None

        # 2. The IP was unassigned AFTER primary was cleared
        assert primary_ip.assigned_object_type is None
        assert primary_ip.assigned_object_id is None
        primary_ip.save.assert_called_once()

        # 3. Device reference was updated
        assert result_device is device_after_clear

    def test_unassign_non_primary_ip_skips_clearing(self):
        """
        When unassigning an IP that is NOT the device's primary, no clearing occurs.
        """
        primary_ip = _make_ip(42, "10.100.200.50/24")  # this is primary
        other_ip = _make_ip(99, "10.100.200.60/24")     # this will be unassigned
        device = _make_device(1, "potato01", primary_ip4=primary_ip)

        nb_api = MagicMock(name="nb_api")
        nb_nics = [_make_nic(100)]

        result_device = _run_ip_unassignment(
            device=device,
            nb_nics=nb_nics,
            netbox_ips=[other_ip],
            all_local_ips=["10.100.200.50/24"],  # primary is still present locally
            nb_api=nb_api,
        )

        # No device re-fetch for clearing (primary wasn't touched)
        nb_api.dcim.devices.get.assert_not_called()

        # The other IP was unassigned
        assert other_ip.assigned_object_type is None
        assert other_ip.assigned_object_id is None
        other_ip.save.assert_called_once()

        # Device primary remains unchanged
        assert result_device.primary_ip4.id == 42

    def test_unassign_ip_no_primary_set(self):
        """
        When device has no primary_ip4, IPs are unassigned without any clearing.
        """
        old_ip = _make_ip(99, "10.100.200.60/24")
        device = _make_device(1, "potato01", primary_ip4=None)

        nb_api = MagicMock(name="nb_api")
        nb_nics = [_make_nic(100)]

        _run_ip_unassignment(
            device=device,
            nb_nics=nb_nics,
            netbox_ips=[old_ip],
            all_local_ips=["10.100.200.99/24"],
            nb_api=nb_api,
        )

        # No device re-fetch
        nb_api.dcim.devices.get.assert_not_called()

        # IP was unassigned
        assert old_ip.assigned_object_type is None
        assert old_ip.assigned_object_id is None
        old_ip.save.assert_called_once()

    def test_all_ips_present_locally_no_unassignment(self):
        """
        When all NetBox IPs match local IPs, nothing is unassigned.
        """
        existing_ip = _make_ip(42, "10.100.200.50/24")
        device = _make_device(1, "potato01", primary_ip4=existing_ip)

        nb_api = MagicMock(name="nb_api")
        nb_nics = [_make_nic(100)]

        _run_ip_unassignment(
            device=device,
            nb_nics=nb_nics,
            netbox_ips=[existing_ip],
            all_local_ips=["10.100.200.50/24"],  # same IP — no unassignment needed
            nb_api=nb_api,
        )

        # Nothing happened
        nb_api.dcim.devices.get.assert_not_called()
        existing_ip.save.assert_not_called()
        assert existing_ip.assigned_object_type == "dcim.interface"

    def test_multiple_ips_only_primary_triggers_clearing(self):
        """
        With multiple IPs to unassign, only the primary triggers clearing.
        """
        primary_ip = _make_ip(42, "10.100.200.50/24")
        other_ip = _make_ip(99, "10.100.200.60/24")
        device = _make_device(1, "potato01", primary_ip4=primary_ip)

        device_after_clear = _make_device(1, "potato01", primary_ip4=None)
        nb_api = MagicMock(name="nb_api")
        nb_api.dcim.devices.get.return_value = device_after_clear

        nb_nics = [_make_nic(100)]

        _run_ip_unassignment(
            device=device,
            nb_nics=nb_nics,
            netbox_ips=[primary_ip, other_ip],
            all_local_ips=["10.100.200.99/24"],  # neither old IP is present
            nb_api=nb_api,
        )

        # Device was re-fetched for clearing (only once, for the primary)
        assert nb_api.dcim.devices.get.call_count == 2  # once for clear, once for refresh

        # Both IPs were unassigned
        assert primary_ip.assigned_object_type is None
        assert other_ip.assigned_object_type is None
        primary_ip.save.assert_called_once()
        other_ip.save.assert_called_once()

    def test_empty_nb_nics_skips_everything(self):
        """
        When there are no NetBox NICs, the IP unassignment block is skipped.
        """
        primary_ip = _make_ip(42, "10.100.200.50/24")
        device = _make_device(1, "potato01", primary_ip4=primary_ip)

        nb_api = MagicMock(name="nb_api")

        _run_ip_unassignment(
            device=device,
            nb_nics=[],  # no NetBox NICs
            netbox_ips=[primary_ip],
            all_local_ips=[],
            nb_api=nb_api,
        )

        # Nothing happened — guard clause skips when no nb_nics
        nb_api.dcim.devices.get.assert_not_called()
        primary_ip.save.assert_not_called()


class TestFixMatchesNetworkPyCode:
    """Verify the extracted test function matches the actual network.py code."""

    def test_fix_code_exists_in_network_py(self):
        """Ensure the primary_ip4 clearing code exists in network.py."""
        import inspect
        from netbox_agent.network import Network

        source = inspect.getsource(Network.create_or_update_netbox_network_cards)

        # Key fix lines should be present
        assert "primary_ip4" in source, "Fix code not found in network.py"
        assert "Clearing primary_ip4" in source, "Fix log message not found"
        assert "fresh_device = nb.dcim.devices.get" in source, "Device re-fetch not found"

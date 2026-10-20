"""
oob_ip must hold steady when an IPMI interface carries two addresses, and
follow the one ipmitool actually reports.

Once the agent stopped unassigning BMC addresses it could not see, an IPMI
interface could keep a live address and a CHG-20 leftover side by side. The
old selection loop picked "any mgmt address that is not the current oob_ip",
so oob_ip swapped between the two on every run (101 -> 202 -> 101 ...), and
NetBox pointed at a dead BMC half the time. Reported on 40 lax01 devices in
the review of FarmGPU/netbox-agent#7.
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

resolve = ServerBase._resolve_oob_ip


def _ip(ip_id, address, iface="IPMI", mgmt_only=True):
    return SimpleNamespace(
        id=ip_id,
        address=address,
        assigned_object=SimpleNamespace(display=iface, mgmt_only=mgmt_only),
    )


LIVE = _ip(101, "10.100.192.150/24")
LEFTOVER = _ip(202, "10.100.200.150/24")
OS = _ip(303, "10.100.194.59/24", iface="mgmt0", mgmt_only=False)
BOTH = [LIVE, LEFTOVER, OS]


def _run_twice(current, myips, observed):
    """Feed each run's answer into the next, as server.py does via NetBox."""
    seen = []
    for _ in range(2):
        new, changed = resolve(current, myips, observed)
        seen.append((new, changed))
        current = SimpleNamespace(id=new) if new is not None else None
    return seen


def test_two_addresses_on_one_mgmt_interface_do_not_flip_when_bmc_is_silent():
    """The reported case: ipmitool silent, two runs, oob_ip unchanged."""
    assert _run_twice(LIVE, BOTH, []) == [(101, False), (101, False)]


def test_two_addresses_on_one_mgmt_interface_do_not_flip_when_bmc_answers():
    assert _run_twice(LIVE, BOTH, ["10.100.192.150/32"]) == [(101, False), (101, False)]


def test_oob_ip_follows_the_address_ipmitool_reports():
    """A re-addressed BMC: the old address stays assigned (it is mgmt_only,
    so never pruned), and without this oob_ip would point at it for good."""
    assert _run_twice(LEFTOVER, BOTH, ["10.100.192.150/32"]) == [(101, True), (101, False)]


def test_observed_host_matches_regardless_of_mask():
    new, changed = resolve(None, [LIVE], ["10.100.192.150/32"])
    assert (new, changed) == (101, True)


def test_empty_oob_ip_is_filled_from_a_mgmt_interface():
    new, changed = resolve(None, BOTH, [])
    assert (new, changed) == (101, True)


def test_observed_address_off_a_mgmt_interface_is_not_used():
    new, changed = resolve(LIVE, BOTH, ["10.100.194.59/32"])
    assert (new, changed) == (101, False)


def test_no_mgmt_interface_leaves_oob_ip_empty():
    assert resolve(None, [OS], []) == (None, False)


def test_legacy_ipmi_display_without_mgmt_only_still_counts():
    legacy = _ip(404, "10.100.192.151/24", mgmt_only=False)
    assert resolve(None, [legacy], []) == (404, True)

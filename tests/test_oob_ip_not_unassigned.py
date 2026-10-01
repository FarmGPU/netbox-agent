"""The in-band agent must not detach a device's out-of-band address.

Three lax01 devices (ash048-2012, ash079-2031, ash109-202j) lost their
oob_ip overnight on 2026-09-30. The address lives on an interface named
IPMI, which the agent only learns about by shelling out to `ipmitool lan
print`; when that returns nothing -- a busy BMC, a missing binary, a host
we have no in-band access to at all -- the address is simply absent from
`all_local_ips` and the unassign loop treats the silence as proof the
record is wrong.

NetBox already refuses to unassign an IP designated as a device's oob_ip.
The agent defeated that guard by nulling oob_ip first, so the only thing
standing between a flaky ipmitool and a destroyed OOB record was removed
on the way past. The changelog shows the flapping that produced:
10.0.25.89 detached 04:59:35, back 04:59:41, detached again 08:22:26.

These exercise the real create_or_update_netbox_network_cards() rather
than a copy of it -- the previous test for this area asserted against a
transcribed copy of the loop, which is why the regression landed clean.
"""

import sys
from collections import defaultdict
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Pre-mock netbox_agent.config — importing it for real parses sys.argv at
# import time, which pytest's own flags break. Same approach as
# tests/test_ip_vrf_and_dns_name.py.
# ---------------------------------------------------------------------------
_mock_nb = MagicMock(name="nb")
_mock_config = SimpleNamespace(
    update_all=True,
    update_network=True,
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

from netbox_agent import network as net  # noqa: E402

nbmock = None


@pytest.fixture(autouse=True)
def _isolate_module_state(monkeypatch):
    """Fresh NetBox mock per test; network.py binds `nb` at import time."""
    global nbmock
    nbmock = MagicMock(name="nb")
    nbmock.version = "4.3.0"
    monkeypatch.setattr(net, "nb", nbmock)
    monkeypatch.setattr(net, "config", _mock_config)
    yield
    nbmock = None


def _ip(rec_id, address):
    rec = MagicMock(name=f"ip-{rec_id}")
    rec.id = rec_id
    rec.address = address
    return rec


class _Device:
    """A device record. Re-fetching one returns the same object, as NetBox's
    would return the same row, so a cleared field is visible on both."""

    def __init__(self, dev_id, name, primary_ip4, oob_ip):
        self.id = dev_id
        self.name = name
        self.primary_ip4 = primary_ip4
        self.oob_ip = oob_ip
        self.save = MagicMock(name=f"{name}.save")


def _device(dev_id=7, name="ash079-2031", primary_ip4=None, oob_ip=None):
    return _Device(dev_id, name, primary_ip4, oob_ip)


def _nb_nic(nic_id, name):
    nic = MagicMock(name=f"nb-nic-{name}")
    nic.id = nic_id
    nic.name = name
    nic.custom_fields = {"managed_by": "netbox-agent"}
    return nic


def _net_obj(device, nb_nics, netbox_ips, local_nics):
    """A ServerNetwork wired up just enough to run the unassign loop.

    `local_nics` is what the host reported this run; leaving the IPMI
    entry out of it is exactly what a failed `ipmitool lan print` looks
    like from inside create_or_update_netbox_network_cards().
    """
    obj = net.ServerNetwork.__new__(net.ServerNetwork)
    obj.device = device
    obj.nics = local_nics
    obj.server = SimpleNamespace(get_hostname=lambda: device.name)
    obj.tenant = None
    obj.lldp = None
    obj.ipmi = None
    # Any interface type resolves to "other"; the NIC half is scaffolding here.
    obj.dcim_choices = {"interface:type": defaultdict(lambda: "other")}
    obj.ipam_choices = {"ip-address:status": defaultdict(lambda: "active")}
    obj.nb_net = nbmock.dcim
    obj.custom_arg = {"device": device.id}
    obj.custom_arg_id = {"device_id": device.id}
    obj.intf_type = "interface_id"
    obj.assigned_object_type = "dcim.interface"
    obj.get_netbox_network_cards = lambda: list(nb_nics)

    def _stop(nic):
        raise _ReachedNicUpdate

    obj.get_netbox_network_card = _stop

    nbmock.ipam.ip_addresses.filter.return_value = list(netbox_ips)
    nbmock.dcim.devices.get.return_value = device
    return obj


class _ReachedNicUpdate(Exception):
    """Raised at the first call of the NIC-update half.

    Everything these tests care about happens in the IP-unassignment loop
    above it. Standing the whole NIC-update half up would mean mocking
    interface types, LAG resolution and VLAN state -- scaffolding that
    tests nothing here and rots. Stopping at the boundary keeps the loop
    itself running as the real method, which the copy-based test this
    replaces did not.
    """


def _run(obj):
    """Run the method and report whether it got past the unassign loop."""
    try:
        obj.create_or_update_netbox_network_cards()
    except _ReachedNicUpdate:
        return True
    return False


# Same shape IPMI.parse() and Network.scan() hand back.
HOST_NIC = {
    "name": "eno1",
    "mac": "AA:BB:CC:00:00:01",
    "ip": ["10.0.1.5/24"],
    "vlan": None,
    "mtu": 1500,
    "bonding": False,
    "ipmi": False,
}
IPMI_NIC = {
    "name": "IPMI",
    "mac": "AA:BB:CC:00:00:99",
    "ip": ["10.0.25.89/32"],
    "vlan": None,
    "mtu": 1500,
    "bonding": False,
    "ipmi": True,
}


class TestTheOobAddressSurvivesABlindRun:
    def test_an_unseen_oob_ip_is_left_assigned(self):
        """ipmitool said nothing; that is not a reason to detach the record."""
        oob = _ip(501, "10.0.25.89/32")
        device = _device(oob_ip=oob)
        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), _nb_nic(2, "IPMI")],
            netbox_ips=[oob],
            local_nics=[HOST_NIC],  # no IPMI entry this run
        )

        _run(obj)

        assert not oob.save.called, "the OOB address was detached"
        assert oob.assigned_object_type is not None

    def test_the_oob_designation_is_not_cleared_to_force_it_through(self):
        """The guard NetBox provides must be left standing."""
        oob = _ip(501, "10.0.25.89/32")
        device = _device(oob_ip=oob)
        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), _nb_nic(2, "IPMI")],
            netbox_ips=[oob],
            local_nics=[HOST_NIC],
        )

        _run(obj)

        assert device.oob_ip is oob
        assert not device.save.called, "the device was written to at all"

    def test_an_oob_ip_the_host_can_see_is_not_touched_either(self):
        """ipmitool worked, so the address matches and never reaches the loop."""
        oob = _ip(501, "10.0.25.89/32")
        device = _device(oob_ip=oob)
        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), _nb_nic(2, "IPMI")],
            netbox_ips=[oob],
            local_nics=[HOST_NIC, IPMI_NIC],
        )

        _run(obj)

        assert not oob.save.called
        assert device.oob_ip is oob


class TestOrdinaryStaleAddressesAreStillPruned:
    def test_an_undesignated_address_is_unassigned(self):
        """The loop still does its job for addresses nothing depends on."""
        stale = _ip(502, "10.0.1.99/24")
        device = _device()
        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1")],
            netbox_ips=[stale],
            local_nics=[HOST_NIC],
        )

        _run(obj)

        assert stale.save.called
        assert stale.assigned_object_type is None
        assert stale.assigned_object_id is None

    def test_a_stale_primary_is_still_cleared_then_unassigned(self):
        """primary_ip4 is in-band data, so the agent remains its owner."""
        primary = _ip(503, "10.0.1.6/24")
        device = _device(primary_ip4=primary)
        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1")],
            netbox_ips=[primary],
            local_nics=[HOST_NIC],
        )

        _run(obj)

        assert device.primary_ip4 is None
        assert primary.save.called
        assert primary.assigned_object_type is None


class TestAFailedPrimaryClearDoesNotEscalate:
    def test_the_address_is_left_alone_rather_than_stripping_oob_too(self):
        """The old code answered an oob_ip validation error by nulling oob_ip.

        That is the escalation that cost us three devices: a save refused
        for one reason became permission to remove a second designation.
        """
        primary = _ip(504, "10.0.1.6/24")
        oob = _ip(505, "10.0.25.89/32")
        device = _device(primary_ip4=primary, oob_ip=oob)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1")],
            netbox_ips=[primary],
            local_nics=[HOST_NIC],
        )

        fresh = MagicMock(name="fresh-device")
        fresh.id = device.id
        fresh.name = device.name
        fresh.save.side_effect = Exception(
            "oob_ip: Cannot reassign IP address while it is designated as "
            "the out-of-band IP for the parent device"
        )
        nbmock.dcim.devices.get.return_value = fresh

        _run(obj)

        # The old code set oob_ip = None and saved a second time. One save
        # attempt, one refusal, and we stop.
        assert fresh.save.call_count == 1, "retried the save after stripping oob_ip"
        assert device.oob_ip is oob, "oob_ip was stripped to force the save"
        assert not primary.save.called, "unassigned anyway after a failed clear"

    def test_a_failed_clear_does_not_abort_the_rest_of_the_sync(self):
        """One stubborn address must not cost us the other updates (SW-393)."""
        primary = _ip(504, "10.0.1.6/24")
        device = _device(primary_ip4=primary)
        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1")],
            netbox_ips=[primary],
            local_nics=[HOST_NIC],
        )
        fresh = MagicMock(name="fresh-device")
        fresh.id = device.id
        fresh.name = device.name
        fresh.save.side_effect = Exception("nope")
        nbmock.dcim.devices.get.return_value = fresh

        assert _run(obj), "the sync stopped instead of carrying on"

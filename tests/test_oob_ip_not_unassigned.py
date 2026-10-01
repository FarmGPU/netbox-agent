"""The in-band agent must not destroy the OOB address it cannot see.

Six lax01 devices lost their `oob_ip` and stayed that way. The address
lives on an interface named IPMI, which the agent only learns about by
shelling out to `ipmitool lan print`. When that is silent -- a busy BMC,
a missing binary, a BMC reporting 0.0.0.0, a host we have no in-band
access to at all -- the agent treated the silence as proof the record was
wrong, and had two ways to act on it:

  * the interface-deletion loop removed the IPMI interface outright,
    taking its IP record and the oob_ip designation with it; and
  * the IP-unassignment loop detached the address, clearing oob_ip first
    to get past the guard NetBox puts there precisely to stop this.

Both are now refused. In band is not authoritative for the out-of-band
address.

These exercise the real create_or_update_netbox_network_cards(). The
file this replaces, tests/test_network_ip.py, asserted against a
hand-transcribed COPY of the loop and checked that a log string appeared
in network.py's source -- which is why it stayed green through the
regression that caused the data loss.

Fidelity rules this file keeps, each one learned from a way the first
draft lied:

  * `ip_addresses.filter` honours the interface ids it is handed. An
    unconditional return hands the loop an address belonging to an
    interface the code deleted moments earlier, and two tests that are
    the whole point of the change then pass against the broken code.
  * every NIC sets `mgmt_only` explicitly. A bare MagicMock attribute is
    truthy, which would satisfy the new deletion guard for no reason.
  * assertions name the object production actually writes to. The device
    is re-fetched before it is modified, so asserting on the original
    instance passes while the re-fetched one is being destroyed.
  * a save that must fail fails the way NetBox fails -- refusing while
    the designation is set, succeeding once it is cleared -- so a test
    goes red on an assertion rather than on an uncaught exception.
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _ip(rec_id, address, iface_id):
    """An IP record that knows which interface it is assigned to.

    The loop filters by interface id, so the assignment has to be real
    here or the filter cannot be honest.
    """
    rec = MagicMock(name=f"ip-{rec_id}")
    rec.id = rec_id
    rec.address = address
    rec.assigned_object_id = iface_id
    rec.assigned_object_type = "dcim.interface"
    return rec


class _Device:
    """A device record.

    Production re-fetches the device before writing to it, so tests must
    be explicit about whether a re-fetch returns this same row or a
    different object. `_net_obj` points `devices.get` here by default;
    tests that care pass their own.
    """

    def __init__(self, dev_id, name, primary_ip4, oob_ip):
        self.id = dev_id
        self.name = name
        self.primary_ip4 = primary_ip4
        self.oob_ip = oob_ip
        self.save = MagicMock(name=f"{name}.save")


def _device(dev_id=7, name="ash079-2031", primary_ip4=None, oob_ip=None):
    return _Device(dev_id, name, primary_ip4, oob_ip)


def _nb_nic(nic_id, name, mgmt_only=False, managed_by="netbox-agent"):
    """A NetBox interface record.

    `mgmt_only` is always explicit: left to a bare MagicMock it is truthy,
    which would satisfy the deletion guard by accident. At lax01 all 129
    interfaces named IPMI are mgmt_only=True and nothing else is.
    """
    nic = MagicMock(name=f"nb-nic-{name}")
    nic.id = nic_id
    nic.name = name
    nic.mgmt_only = mgmt_only
    nic.custom_fields = {"managed_by": managed_by}
    return nic


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
    "ip": ["10.0.25.89/24"],
    "vlan": None,
    "mtu": 1500,
    "bonding": False,
    "ipmi": True,
}
# What IPMI.parse() returns when the BMC answers but reports 0.0.0.0, or
# when ignore_ips matches: name and MAC, no address (ipmi.py sets ip_list
# to []). This is the shape behind the lax01 losses -- the interface is in
# local_nics so it survives the deletion loop, but the address is missing
# from all_local_ips, so the unassign loop detaches it and nothing in the
# NIC-update half puts it back (network.py guards that on `if nic["ip"]`).
IPMI_NIC_NO_ADDRESS = dict(IPMI_NIC, ip=[])


class _ReachedNicUpdate(Exception):
    """Raised at the first call of the NIC-update half.

    Everything these tests care about happens in the interface-deletion
    and IP-unassignment loops above it. Standing the NIC-update half up
    would mean mocking interface types, LAG resolution and VLAN state --
    scaffolding that tests nothing here and rots. Stopping at the
    boundary keeps both loops running as the real method.
    """


def _run(obj):
    """Run the method; return True if it got past the loops under test.

    Every test asserts this, so a method that died mid-loop is never
    mistaken for one that ran and chose not to write.
    """
    try:
        obj.create_or_update_netbox_network_cards()
    except _ReachedNicUpdate:
        return True
    return False


def _net_obj(device, nb_nics, netbox_ips, local_nics, refetch=None):
    """A ServerNetwork wired up just enough to run the two loops.

    `local_nics` is what the host reported this run; leaving the IPMI
    entry out is exactly what a silent `ipmitool lan print` looks like
    from inside create_or_update_netbox_network_cards().
    """
    obj = net.ServerNetwork.__new__(net.ServerNetwork)
    obj.device = device
    obj.nics = local_nics
    obj.server = SimpleNamespace(get_hostname=lambda: device.name)
    obj.tenant = None
    obj.lldp = None
    obj.ipmi = None
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

    def _filter(**kwargs):
        """Honour the interface ids, as the real endpoint does.

        An address whose interface was deleted above is gone from this
        result, which is the whole point: the unassign loop must not be
        handed a row the deletion loop already took away.
        """
        ids = kwargs.get("interface_id") or ()
        return [ip for ip in netbox_ips if ip.assigned_object_id in tuple(ids)]

    nbmock.ipam.ip_addresses.filter.side_effect = _filter
    nbmock.dcim.devices.get.return_value = device if refetch is None else refetch

    # _oob_interface_id() reads the oob_ip record back to learn which
    # interface carries it; mirror the fixture rather than inventing one.
    def _ip_get(ip_id):
        for ip in netbox_ips:
            if ip.id == ip_id:
                return ip
        return None

    nbmock.ipam.ip_addresses.get.side_effect = _ip_get
    return obj


# ---------------------------------------------------------------------------
# The interface-deletion loop
# ---------------------------------------------------------------------------


class TestTheOobInterfaceSurvivesABlindRun:
    def test_the_ipmi_interface_is_not_deleted_when_ipmitool_is_silent(self):
        """Deleting it takes the IP record and the designation with it."""
        ipmi_nic = _nb_nic(2, "IPMI", mgmt_only=True)
        oob = _ip(501, "10.0.25.89/24", iface_id=2)
        device = _device(oob_ip=oob)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), ipmi_nic],
            netbox_ips=[oob],
            local_nics=[HOST_NIC],  # no IPMI entry this run
        )

        assert _run(obj)
        assert not ipmi_nic.delete.called, "the OOB interface was deleted"

    def test_an_ordinary_interface_the_host_stopped_reporting_is_deleted(self):
        """The pruning the loop exists for still happens."""
        gone = _nb_nic(3, "eno2", mgmt_only=False)
        device = _device()

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), gone],
            netbox_ips=[],
            local_nics=[HOST_NIC],
        )

        assert _run(obj)
        assert gone.delete.called, "a genuinely stale interface was kept"

    def test_an_interface_carrying_the_oob_ip_is_kept_even_if_not_mgmt_only(self):
        """Two independent grounds; neither leans on the other."""
        odd = _nb_nic(4, "bmc0", mgmt_only=False)
        oob = _ip(502, "10.0.25.90/24", iface_id=4)
        device = _device(oob_ip=oob)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), odd],
            netbox_ips=[oob],
            local_nics=[HOST_NIC],
        )

        assert _run(obj)
        assert not odd.delete.called


# ---------------------------------------------------------------------------
# The IP-unassignment loop
# ---------------------------------------------------------------------------


class TestTheOobAddressSurvivesABlindRun:
    def test_an_oob_ip_the_bmc_did_not_report_is_left_assigned(self):
        """The BMC answered but gave no address; that is not a reason to detach.

        The interface survives here -- it is in local_nics -- so the
        address really does reach the unassign loop. That matters: in the
        fully-silent case the interface is deleted instead, and a test
        written that way passes against the unfixed code for the wrong
        reason, because nothing ever reaches the loop to be spared.
        """
        ipmi_nic = _nb_nic(2, "IPMI", mgmt_only=True)
        oob = _ip(501, "10.0.25.89/24", iface_id=2)
        device = _device(oob_ip=oob)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), ipmi_nic],
            netbox_ips=[oob],
            local_nics=[HOST_NIC, IPMI_NIC_NO_ADDRESS],
        )

        assert _run(obj)
        assert not ipmi_nic.delete.called, "the interface went instead"
        assert not oob.save.called, "the OOB address was detached"
        assert oob.assigned_object_id == 2

    def test_the_oob_designation_is_not_cleared_to_force_it_through(self):
        """The guard NetBox provides must be left standing.

        Asserted on the re-fetched row as well as the original, because
        production writes to the re-fetch and only that one would show
        the damage.
        """
        ipmi_nic = _nb_nic(2, "IPMI", mgmt_only=True)
        oob = _ip(501, "10.0.25.89/24", iface_id=2)
        device = _device(oob_ip=oob)
        refetch = _Device(device.id, device.name, None, oob)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), ipmi_nic],
            netbox_ips=[oob],
            local_nics=[HOST_NIC, IPMI_NIC_NO_ADDRESS],
            refetch=refetch,
        )

        assert _run(obj)
        assert device.oob_ip is oob
        assert refetch.oob_ip is oob, "oob_ip was cleared on the re-fetched row"
        assert not refetch.save.called, "the device was written to at all"

    def test_an_oob_ip_the_host_can_see_is_not_touched_either(self):
        """ipmitool worked, so the address matches and never reaches the guard."""
        ipmi_nic = _nb_nic(2, "IPMI", mgmt_only=True)
        oob = _ip(501, "10.0.25.89/24", iface_id=2)
        device = _device(oob_ip=oob)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), ipmi_nic],
            netbox_ips=[oob],
            local_nics=[HOST_NIC, IPMI_NIC],
        )

        assert _run(obj)
        assert not oob.save.called
        assert device.oob_ip is oob


class TestOrdinaryStaleAddressesAreStillPruned:
    def test_an_undesignated_address_is_unassigned(self):
        """The loop still does its job for addresses nothing depends on."""
        stale = _ip(502, "10.0.1.99/24", iface_id=1)
        device = _device()

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1")],
            netbox_ips=[stale],
            local_nics=[HOST_NIC],
        )

        assert _run(obj)
        assert stale.save.called
        assert stale.assigned_object_type is None
        assert stale.assigned_object_id is None

    def test_a_stale_address_is_pruned_on_a_device_that_has_an_oob_ip(self):
        """The guard is scoped to the designated address, not to the device.

        Without this, dropping the `device_oob.id == netbox_ip.id` half of
        the condition would only be caught by an unrelated test.
        """
        ipmi_nic = _nb_nic(2, "IPMI", mgmt_only=True)
        oob = _ip(501, "10.0.25.89/24", iface_id=2)
        stale = _ip(502, "10.0.1.99/24", iface_id=1)
        device = _device(oob_ip=oob)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), ipmi_nic],
            netbox_ips=[oob, stale],
            local_nics=[HOST_NIC],
        )

        assert _run(obj)
        assert not oob.save.called, "the designated address was detached"
        assert stale.save.called, "an ordinary stale address was spared"

    def test_a_stale_primary_is_still_cleared_then_unassigned(self):
        """primary_ip4 is in-band data, so the agent remains its owner."""
        primary = _ip(503, "10.0.1.6/24", iface_id=1)
        device = _device(primary_ip4=primary)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1")],
            netbox_ips=[primary],
            local_nics=[HOST_NIC],
        )

        assert _run(obj)
        assert device.primary_ip4 is None
        assert primary.save.called
        assert primary.assigned_object_type is None


class TestAFailedPrimaryClearDoesNotEscalate:
    def test_the_address_is_left_alone_rather_than_stripping_oob_too(self):
        """The old code answered an oob_ip validation error by nulling oob_ip.

        That is the escalation that cost us six devices: a save refused
        for one reason became permission to remove a second designation.
        The save here refuses the way NetBox refuses -- while the
        designation is set, and not once it is gone -- so the old code
        succeeds at destroying it and this test goes red on an assertion
        rather than on an uncaught exception.
        """
        primary = _ip(504, "10.0.1.6/24", iface_id=1)
        oob = _ip(505, "10.0.25.89/24", iface_id=2)
        device = _device(primary_ip4=primary, oob_ip=oob)
        refetch = _Device(device.id, device.name, primary, oob)

        def _save():
            if refetch.oob_ip is not None:
                raise Exception(
                    "oob_ip: Cannot reassign IP address while it is designated "
                    "as the out-of-band IP for the parent device"
                )
            return True

        refetch.save.side_effect = _save

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), _nb_nic(2, "IPMI", mgmt_only=True)],
            netbox_ips=[primary],
            local_nics=[HOST_NIC],
            refetch=refetch,
        )

        assert _run(obj)
        assert refetch.oob_ip is oob, "oob_ip was stripped to force the save"
        assert refetch.save.call_count == 1, "retried the save after stripping oob_ip"
        assert not primary.save.called, "unassigned anyway after a failed clear"

    def test_a_failed_clear_does_not_abort_the_rest_of_the_sync(self):
        """One stubborn address must not cost us the other updates (SW-393)."""
        primary = _ip(504, "10.0.1.6/24", iface_id=1)
        device = _device(primary_ip4=primary)
        refetch = _Device(device.id, device.name, primary, None)
        refetch.save.side_effect = Exception("nope")

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1")],
            netbox_ips=[primary],
            local_nics=[HOST_NIC],
            refetch=refetch,
        )

        assert _run(obj), "the sync stopped instead of carrying on"

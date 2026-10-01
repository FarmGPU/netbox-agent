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

    `saved` records primary_ip4 as each save() sent it, because the
    attribute alone cannot tell a value that reached NetBox from one that
    was only ever assigned in memory -- a drop of the save() call leaves
    the attribute looking exactly right. A test that installs its own
    side_effect takes that recording over.
    """

    def __init__(self, dev_id, name, primary_ip4, oob_ip):
        self.id = dev_id
        self.name = name
        self.primary_ip4 = primary_ip4
        self.oob_ip = oob_ip
        self.saved = []
        self.save = MagicMock(
            name=f"{name}.save", side_effect=lambda: self.saved.append(self.primary_ip4)
        )


def _device(dev_id=7, name="ash079-2031", primary_ip4=None, oob_ip=None):
    return _Device(dev_id, name, primary_ip4, oob_ip)


class _BriefWhoseIdRaises:
    """A nested brief that fetches when asked for a field it does not hold."""

    address = "10.0.25.89/24"

    @property
    def id(self):
        raise Exception("503 Service Unavailable")


class _DeviceWhoseOobIpRaises(_Device):
    """Reading oob_ip is a network call on a record that lacks the field.

    pynetbox resolves an absent attribute by fetching the object, so the
    failure arrives as a transport error rather than an AttributeError and
    the getattr default does not catch it.
    """

    def __init__(self, dev_id=7, name="ash079-2031"):
        super().__init__(dev_id, name, None, None)

    @property
    def oob_ip(self):
        raise Exception("503 Service Unavailable")

    @oob_ip.setter
    def oob_ip(self, value):
        pass


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

    # _oob_ids() reads the oob_ip record back to learn which
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
        """Deleting it takes the IP record and the designation with it.

        The device deliberately has no oob_ip here. That is the state the
        six real devices were left in, and it is the only way this test
        measures the mgmt_only guard rather than the oob-carrier one --
        with both in play, removing either leaves the other to pass the
        test and the mutation goes unnoticed.
        """
        ipmi_nic = _nb_nic(2, "IPMI", mgmt_only=True)
        bmc_address = _ip(501, "10.0.25.89/24", iface_id=2)
        device = _device(oob_ip=None)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), ipmi_nic],
            netbox_ips=[bmc_address],
            local_nics=[HOST_NIC],  # no IPMI entry this run
        )

        assert _run(obj)
        assert not ipmi_nic.delete.called, "the OOB interface was deleted"
        assert not bmc_address.save.called, "its address was detached instead"

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


class TestAnUnanswerableLookupFailsClosed:
    """ "I could not read it" must not be treated as "there is nothing there".

    `_oob_ids()` answers what the oob_ip designation protects. If a
    transient NetBox read turns that question into None, the deletion loop
    reads it as "this device has no oob_ip" and is free to delete the very
    interface the lookup exists to save -- and a non-mgmt_only one has no
    second line of defence.
    """

    def test_nothing_is_pruned_when_the_oob_lookup_raises(self):
        odd = _nb_nic(4, "bmc0", mgmt_only=False)
        stale = _nb_nic(3, "eno2", mgmt_only=False)
        oob = _ip(502, "10.0.25.90/24", iface_id=4)
        device = _device(oob_ip=oob)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), odd, stale],
            netbox_ips=[oob],
            local_nics=[HOST_NIC],
        )
        nbmock.ipam.ip_addresses.get.side_effect = Exception("502 Bad Gateway")

        assert _run(obj)
        assert not odd.delete.called, "deleted the OOB interface on a read failure"
        # The stale one is kept too. That is the point: we cannot tell them
        # apart this run, so we take the recoverable mistake.
        assert not stale.delete.called

    def test_nothing_is_pruned_when_the_oob_ip_has_no_usable_id(self):
        odd = _nb_nic(4, "bmc0", mgmt_only=False)
        device = _device(oob_ip=SimpleNamespace(address="10.0.25.90/24"))

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), odd],
            netbox_ips=[],
            local_nics=[HOST_NIC],
        )

        assert _run(obj)
        assert not odd.delete.called

    def test_a_malformed_oob_ip_leaves_every_address_alone(self):
        """Not knowing which address is designated protects all of them.

        The earlier shape of this guard read the id with a getattr default
        and compared None to the record id -- so an unreadable designation
        quietly matched nothing and the address was detached. An
        AttributeError would at least have been loud; this was silent, and
        it was the outcome the whole change exists to prevent.
        """
        ipmi_nic = _nb_nic(2, "IPMI", mgmt_only=True)
        oob = _ip(501, "10.0.25.90/24", iface_id=2)
        stale = _ip(502, "10.0.1.99/24", iface_id=1)
        device = _device(oob_ip=SimpleNamespace(address="10.0.25.90/24"))

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), ipmi_nic],
            netbox_ips=[oob, stale],
            local_nics=[HOST_NIC],
        )

        assert _run(obj), "the sync died on a malformed oob_ip"
        assert not ipmi_nic.delete.called, "destroyed the interface instead"
        assert not oob.save.called, "detached the address it could not identify"
        assert not stale.save.called, "touched anything at all while blind"

    def test_a_failed_read_back_still_lets_ordinary_addresses_be_pruned(self):
        """Only the interface answer is lost; the address answer survives.

        The brief names the designated address without a round trip, so a
        read-back that fails costs us pruning, not the whole loop.
        """
        oob = _ip(501, "10.0.25.90/24", iface_id=2)
        stale = _ip(502, "10.0.1.99/24", iface_id=1)
        device = _device(oob_ip=oob)

        obj = _net_obj(
            device,
            # iface 2 has to be here or `oob` is filtered out before any
            # guard runs and the assertion below cannot fail.
            nb_nics=[
                _nb_nic(1, "eno1"),
                _nb_nic(2, "IPMI", mgmt_only=True),
                _nb_nic(3, "eno2", mgmt_only=False),
            ],
            netbox_ips=[oob, stale],
            local_nics=[HOST_NIC],
        )
        nbmock.ipam.ip_addresses.get.side_effect = Exception("504 Gateway Timeout")

        assert _run(obj)
        assert not oob.save.called, "detached the designated address"
        assert stale.save.called, "stopped pruning addresses as well"

    def test_an_oob_ip_assigned_to_another_kind_of_object_protects_no_interface(self):
        """An id from another id space must not match one of our interfaces."""
        elsewhere = _ip(501, "10.0.25.90/24", iface_id=3)
        elsewhere.assigned_object_type = "virtualization.vminterface"
        stale = _nb_nic(3, "eno2", mgmt_only=False)
        device = _device(oob_ip=elsewhere)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), stale],
            netbox_ips=[elsewhere],
            local_nics=[HOST_NIC],
        )

        assert _run(obj)
        assert stale.delete.called, "kept an unrelated interface by id collision"

    def test_a_record_assigned_to_nothing_does_not_wedge_pruning(self):
        gone = _nb_nic(3, "eno2", mgmt_only=False)
        floating = _ip(501, "10.0.25.90/24", iface_id=None)
        device = _device(oob_ip=floating)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), gone],
            netbox_ips=[floating],
            local_nics=[HOST_NIC],
        )

        assert _run(obj)
        assert gone.delete.called

    def test_pruning_proceeds_when_the_oob_ip_record_is_simply_gone(self):
        """A dangling designation is an answer, not a failure.

        Nothing we delete can make an already-missing record worse, so this
        must not wedge pruning for the device forever.
        """
        stale = _nb_nic(3, "eno2", mgmt_only=False)
        oob = _ip(502, "10.0.25.90/24", iface_id=4)
        device = _device(oob_ip=oob)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), stale],
            netbox_ips=[],
            local_nics=[HOST_NIC],
        )
        nbmock.ipam.ip_addresses.get.side_effect = lambda _id: None

        assert _run(obj)
        assert stale.delete.called, "pruning stayed wedged on a dangling oob_ip"


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

        Two guards cover this address, and the mgmt_only one runs first.
        That is the real-world arrangement and worth testing as such; the
        oob guard is isolated separately, on a BMC whose interface nobody
        marked mgmt_only.
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
        the damage. As above, the mgmt_only guard is what spares this
        particular address; the oob guard has its own test.
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
        assert not oob.save.called, "the address was detached instead"
        assert device.oob_ip is oob
        assert refetch.oob_ip is oob, "oob_ip was cleared on the re-fetched row"
        assert not refetch.save.called, "the device was written to at all"

    def test_a_designated_address_on_an_ordinary_interface_is_left_alone(self):
        """The oob guard standing on its own.

        Everywhere else the designated address also sits on a mgmt_only
        interface, so that guard spares it first and this one could be
        deleted without a single test noticing. A BMC reachable over an
        interface nobody marked mgmt_only is the case that tells them
        apart.
        """
        bmc0 = _nb_nic(4, "bmc0", mgmt_only=False)
        oob = _ip(501, "10.0.25.90/24", iface_id=4)
        device = _device(oob_ip=oob)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), bmc0],
            netbox_ips=[oob],
            local_nics=[HOST_NIC],
        )

        assert _run(obj)
        assert not bmc0.delete.called, "the interface went instead"
        assert not oob.save.called, "the designated address was detached"

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
        """primary_ip4 is in-band data, so the agent remains its owner.

        The re-read returns a distinct object so the reference update is
        actually observable; pointed at the same row it would pass whether
        or not the assignment happened.
        """
        primary = _ip(503, "10.0.1.6/24", iface_id=1)
        device = _device(primary_ip4=primary)
        refetch = _Device(device.id, device.name, primary, None)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1")],
            netbox_ips=[primary],
            local_nics=[HOST_NIC],
            refetch=refetch,
        )

        assert _run(obj)
        assert refetch.primary_ip4 is None
        assert primary.save.called
        assert primary.assigned_object_type is None
        assert obj.device is refetch, "did not pick up the re-read device"


class TestNothingOnAManagementInterfaceIsOurs:
    """The deletion loop spares these interfaces; the address loop must agree.

    Sparing the interface and then stripping the address off it is the same
    loss by a slower route -- and server.py cannot re-designate an oob_ip
    from an address that is no longer assigned to the device.
    """

    def test_a_bmc_address_that_is_not_the_designated_one_is_left_alone(self):
        """40 of lax01's 169 BMC addresses are in exactly this state."""
        ipmi_nic = _nb_nic(2, "IPMI", mgmt_only=True)
        designated = _ip(501, "10.0.25.89/24", iface_id=2)
        other_bmc = _ip(502, "10.0.6.53/24", iface_id=2)
        device = _device(oob_ip=designated)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), ipmi_nic],
            netbox_ips=[designated, other_bmc],
            local_nics=[HOST_NIC, IPMI_NIC_NO_ADDRESS],
        )

        assert _run(obj)
        assert not ipmi_nic.delete.called, "took the interface instead"
        assert not designated.save.called
        assert not other_bmc.save.called, "stripped a BMC address we cannot see"

    def test_a_bmc_address_on_a_device_with_no_designation_is_left_alone(self):
        """ash088-2020 today: a BMC address and no oob_ip to protect it."""
        ipmi_nic = _nb_nic(2, "IPMI", mgmt_only=True)
        bmc_address = _ip(501, "10.0.6.45/24", iface_id=2)
        device = _device(oob_ip=None)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), ipmi_nic],
            netbox_ips=[bmc_address],
            local_nics=[HOST_NIC, IPMI_NIC_NO_ADDRESS],
        )

        assert _run(obj)
        assert not bmc_address.save.called

    def test_an_ordinary_interfaces_address_is_still_pruned(self):
        """The guard is scoped to mgmt_only, not to every interface."""
        stale = _ip(502, "10.0.1.99/24", iface_id=1)
        device = _device()

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1", mgmt_only=False)],
            netbox_ips=[stale],
            local_nics=[HOST_NIC],
        )

        assert _run(obj)
        assert stale.save.called


class TestTheGuardsRunBeforeAnythingDestructive:
    def test_an_address_that_is_both_primary_and_oob_produces_no_writes(self):
        """Order matters: the oob guard has to come first.

        With the primary_ip4 block ahead of it, the device is re-fetched and
        its primary_ip4 nulled and saved before the guard spares the
        address -- a write to a device we were about to decide not to touch.
        """
        # Deliberately NOT mgmt_only: that guard runs earlier and would
        # spare the address before the ordering under test could matter,
        # leaving the swap undetected.
        both = _ip(501, "10.0.25.89/24", iface_id=4)
        device = _device(primary_ip4=both, oob_ip=both)
        refetch = _Device(device.id, device.name, both, both)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), _nb_nic(4, "bmc0", mgmt_only=False)],
            netbox_ips=[both],
            local_nics=[HOST_NIC],
            refetch=refetch,
        )

        assert _run(obj)
        assert not both.save.called
        assert not refetch.save.called, "wrote to the device before sparing it"
        assert refetch.primary_ip4 is both

    def test_a_re_read_that_raises_costs_one_address_not_the_run(self):
        """The re-fetch is a network call like any other.

        It sits above the try that wraps the save, so a transient failure
        there used to escape the loop entirely -- one unlucky request
        costing every later NIC update on the device.
        """
        primary = _ip(504, "10.0.1.6/24", iface_id=1)
        later = _ip(505, "10.0.1.99/24", iface_id=1)
        device = _device(primary_ip4=primary)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1")],
            netbox_ips=[primary, later],
            local_nics=[HOST_NIC],
        )
        nbmock.dcim.devices.get.side_effect = Exception("503 Service Unavailable")

        assert _run(obj), "a failed re-read stopped the whole sync"
        assert not primary.save.called, "unassigned without clearing primary_ip4"
        assert later.save.called, "the rest of the batch was abandoned"

    def test_a_re_read_that_raises_after_the_clear_keeps_the_old_reference(self):
        """The refresh is best-effort; losing it must not lose the run."""
        primary = _ip(504, "10.0.1.6/24", iface_id=1)
        device = _device(primary_ip4=primary)
        refetch = _Device(device.id, device.name, primary, None)

        calls = {"n": 0}

        def _get(_id):
            calls["n"] += 1
            if calls["n"] == 1:
                return refetch
            raise Exception("503 Service Unavailable")

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1")],
            netbox_ips=[primary],
            local_nics=[HOST_NIC],
        )
        nbmock.dcim.devices.get.side_effect = _get

        assert _run(obj), "a failed refresh stopped the whole sync"
        assert refetch.primary_ip4 is None, "the clear did not happen"
        assert primary.save.called, "the address was not unassigned"
        assert obj.device is device, "dropped the device reference entirely"

    def test_a_refused_unassign_costs_one_address_not_the_run(self):
        """NetBox refuses for reasons we did not anticipate (SW-393)."""
        stubborn = _ip(502, "10.0.1.99/24", iface_id=1)
        stubborn.save.side_effect = Exception(
            "400: Cannot reassign IP address while it is designated as the "
            "out-of-band IP for the parent device"
        )
        device = _device()

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1")],
            netbox_ips=[stubborn],
            local_nics=[HOST_NIC],
        )

        assert _run(obj), "one refused address stopped the whole sync"
        # The local record is put back, so nothing downstream reads it as
        # detached when NetBox still has it assigned.
        assert stubborn.assigned_object_id == 1
        assert stubborn.assigned_object_type == "dcim.interface"


class TestOneBadRecordCostsThatRecord:
    """The claim the rest of the change rests on, tested per call.

    Every destructive or load-bearing call in these two loops is a network
    request. If any of them can take the run down, the device never
    reaches server.py's oob_ip reconvergence -- which is the thing that
    heals a device this bug has already bitten.
    """

    def test_a_failed_delete_leaves_the_other_interfaces_prunable(self):
        stubborn = _nb_nic(3, "eno2", mgmt_only=False)
        stubborn.delete.side_effect = Exception("502 Bad Gateway")
        also_stale = _nb_nic(5, "eno3", mgmt_only=False)
        device = _device()

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), stubborn, also_stale],
            netbox_ips=[],
            local_nics=[HOST_NIC],
        )

        assert _run(obj), "one failed delete stopped the whole sync"
        assert also_stale.delete.called, "the rest of the pruning was abandoned"

    def test_an_interface_we_failed_to_delete_keeps_its_addresses_in_scope(self):
        """It is still in NetBox, so its addresses are still ours to judge.

        Dropping it from nb_nics before the delete succeeded would take
        them out of the batch and quietly exempt them.
        """
        stubborn = _nb_nic(3, "eno2", mgmt_only=False)
        stubborn.delete.side_effect = Exception("502 Bad Gateway")
        orphan = _ip(502, "10.0.1.99/24", iface_id=3)
        device = _device()

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), stubborn],
            netbox_ips=[orphan],
            local_nics=[HOST_NIC],
        )

        assert _run(obj)
        assert orphan.save.called, "its addresses fell out of the batch"

    def test_a_failed_address_read_still_leaves_the_run_to_finish(self):
        """It sits between the deletions and server.py's reconvergence."""
        device = _device()
        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1")],
            netbox_ips=[],
            local_nics=[HOST_NIC],
        )
        nbmock.ipam.ip_addresses.filter.side_effect = Exception("504 Gateway Timeout")

        assert _run(obj), "a failed address read stopped the whole sync"

    def test_a_refused_unassign_puts_primary_ip4_back(self):
        """We nulled it to make room for a write that did not happen."""
        primary = _ip(504, "10.0.1.6/24", iface_id=1)
        primary.save.side_effect = Exception("400: refused for some other reason")
        device = _device(primary_ip4=primary)
        refetch = _Device(device.id, device.name, primary, None)

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1")],
            netbox_ips=[primary],
            local_nics=[HOST_NIC],
            refetch=refetch,
        )

        assert _run(obj)
        assert refetch.saved == [None, primary.id], (
            "the designation was restored in memory but never written back"
        )


class TestADesignationWeCannotEvenReadProtectsEverything:
    def test_an_oob_ip_read_that_raises_leaves_the_device_alone(self):
        """pynetbox fetches an absent field, so this read is a network call.

        It is the one branch of _oob_ids that no fixture could reach while
        oob_ip was a plain attribute, and a device whose designation we
        cannot read at all is the last one to start deleting things on.
        """
        ipmi_nic = _nb_nic(2, "IPMI", mgmt_only=True)
        stale_iface = _nb_nic(3, "eno2", mgmt_only=False)
        bmc_address = _ip(501, "10.0.25.89/24", iface_id=2)
        stale = _ip(502, "10.0.1.99/24", iface_id=3)
        device = _DeviceWhoseOobIpRaises()

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), ipmi_nic, stale_iface],
            netbox_ips=[bmc_address, stale],
            local_nics=[HOST_NIC],
        )

        assert _run(obj), "the sync died rather than backing off"
        assert not ipmi_nic.delete.called
        assert not stale_iface.delete.called, "pruned while unable to read"
        assert not bmc_address.save.called
        assert not stale.save.called, "unassigned while unable to read"

    def test_an_oob_ip_whose_id_raises_leaves_the_device_alone(self):
        """The brief is a record too, and reading a field it lacks fetches.

        This is the branch whose whole job is to fail safe, so it is the
        last place that should be able to raise on the way there.
        """
        stale_iface = _nb_nic(3, "eno2", mgmt_only=False)
        stale = _ip(502, "10.0.1.99/24", iface_id=3)
        device = _device(oob_ip=_BriefWhoseIdRaises())

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), stale_iface],
            netbox_ips=[stale],
            local_nics=[HOST_NIC],
        )

        assert _run(obj), "the sync died reading the designation"
        assert not stale_iface.delete.called, "pruned while unable to read"
        assert not stale.save.called, "unassigned while unable to read"


class TestAnotherWorkersInterfaceIsStillSafe:
    def test_managed_by_someone_else_is_not_deleted(self):
        """Pre-existing guard, and this file is now the only one running it.

        bmc-scan and proxmox-sync create interfaces the OS cannot see; the
        two new guards run ahead of this one, so its own coverage went
        with tests/test_network_ip.py.
        """
        theirs = _nb_nic(3, "eno2", mgmt_only=False, managed_by="bmc-scan")
        device = _device()

        obj = _net_obj(
            device,
            nb_nics=[_nb_nic(1, "eno1"), theirs],
            netbox_ips=[],
            local_nics=[HOST_NIC],
        )

        assert _run(obj)
        assert not theirs.delete.called


class TestTheSentinel:
    def test_it_refuses_to_be_a_boolean(self):
        """`if oob_interface_id:` must not quietly mean "nothing to protect"."""
        with pytest.raises(TypeError):
            bool(net.UNRESOLVED)

    def test_a_virtual_machine_is_re_read_from_its_own_endpoint(self):
        """Reading a VM id from the devices endpoint finds someone else."""
        obj = net.VirtualNetwork.__new__(net.VirtualNetwork)
        obj.device = SimpleNamespace(id=9, name="vm-9")
        obj.assigned_object_type = "virtualization.vminterface"

        obj._refetch_device()

        nbmock.virtualization.virtual_machines.get.assert_called_once_with(9)
        assert not nbmock.dcim.devices.get.called


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

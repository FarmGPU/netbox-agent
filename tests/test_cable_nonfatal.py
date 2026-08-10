"""
Tests that a NetBox-rejected cable does not abort the whole sync.

SW-393: netbox-agent died outright when NetBox refused a cable termination.
The RequestError propagated out of connect_interface_to_switch, up through
create_or_update_netbox_network_cards and netbox_create_or_update, killing
the run before it wrote last_agent_sync — so every module, interface and IP
already gathered was discarded and the device looked untouched.

Trigger seen in the field: a VLAN subinterface (e.g. `1s0f0.234`). NetBox
rejects any termination whose interface type is in NONCONNECTABLE_IFACE_TYPES
(virtual + wireless) at the model layer, unconditionally:

    dcim/models/cables.py:574 -> 400 "Cables cannot be terminated to
                                      Virtual interfaces"

Cabling is enrichment; inventory is the product. A refused cable must
degrade to a logged error, not an aborted sync.
"""

import sys
import pytest
from unittest.mock import MagicMock
from types import SimpleNamespace

from pynetbox.core.query import RequestError


# ---------------------------------------------------------------------------
# Pre-mock netbox_agent.config to avoid import-time sys.argv parsing
# (same approach as test_cable_orphan.py)
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
sys.modules.setdefault("netbox_agent.misc", MagicMock())
sys.modules.setdefault("netbox_agent.ethtool", MagicMock())
sys.modules.setdefault("netbox_agent.lldp", MagicMock())
sys.modules.setdefault("netbox_agent.ipmi", MagicMock())


def _make_server_network():
    from netbox_agent.network import ServerNetwork

    net = ServerNetwork.__new__(ServerNetwork)
    net.lldp = MagicMock()
    net.server = MagicMock()
    return net


def _virtual_iface_request_error():
    """Build the exact RequestError NetBox raises for a virtual termination."""
    req = MagicMock()
    req.status_code = 400
    req.reason = "Bad Request"
    req.json.return_value = {
        "__all__": ["Cables cannot be terminated to Virtual interfaces"]
    }
    req.url = "https://netbox.example.net/api/dcim/cables/"
    req.text = '{"__all__": ["Cables cannot be terminated to Virtual interfaces"]}'
    req.request.body = "{}"
    return RequestError(req)


class TestCableFailureIsNonFatal:

    def setup_method(self):
        _mock_nb.reset_mock()
        _mock_nb.dcim.cables.create.side_effect = None

    def _reach_cable_creation(self, net):
        """Wire the nb mocks so connect_interface_to_switch reaches cables.create()."""
        mock_mgmt_ip = MagicMock()
        mock_mgmt_ip.assigned_object = MagicMock()
        mock_mgmt_ip.assigned_object.device = MagicMock()
        mock_mgmt_ip.assigned_object.device.id = 331

        nb_switch_interface = MagicMock()
        nb_switch_interface.id = 13261
        nb_switch_interface.cable = None

        _mock_nb.ipam.ip_addresses.get.return_value = mock_mgmt_ip
        _mock_nb.dcim.interfaces.get.return_value = nb_switch_interface
        net.lldp.get_switch_port.return_value = "swp58s2"

        nb_server_interface = MagicMock()
        nb_server_interface.name = "1s0f0.234"
        return "10.0.0.1", "swp58s2", nb_server_interface

    def test_rejected_cable_does_not_raise(self):
        """A 400 from cables.create must not propagate to the caller."""
        net = _make_server_network()
        switch_ip, switch_iface, nb_server_interface = self._reach_cable_creation(net)

        _mock_nb.dcim.cables.create.side_effect = _virtual_iface_request_error()

        # Before the fix this raised RequestError and killed the entire run.
        result = net.connect_interface_to_switch(
            switch_ip, switch_iface, nb_server_interface
        )

        assert result is nb_server_interface
        assert _mock_nb.dcim.cables.create.called

    def test_rejected_cable_is_logged_as_error(self, caplog):
        """The failure must be visible, not silently swallowed."""
        net = _make_server_network()
        switch_ip, switch_iface, nb_server_interface = self._reach_cable_creation(net)

        _mock_nb.dcim.cables.create.side_effect = _virtual_iface_request_error()

        with caplog.at_level("ERROR"):
            net.connect_interface_to_switch(switch_ip, switch_iface, nb_server_interface)

        assert "Failed to cable" in caplog.text
        assert "1s0f0.234" in caplog.text
        assert "continuing sync" in caplog.text

    def test_successful_cable_still_assigned(self):
        """The happy path must be unchanged — cable still attached to the interface."""
        net = _make_server_network()
        switch_ip, switch_iface, nb_server_interface = self._reach_cable_creation(net)

        new_cable = MagicMock()
        _mock_nb.dcim.cables.create.return_value = new_cable

        result = net.connect_interface_to_switch(
            switch_ip, switch_iface, nb_server_interface
        )

        assert result is nb_server_interface
        assert nb_server_interface.cable is new_cable

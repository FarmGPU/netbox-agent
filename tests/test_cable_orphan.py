"""
Tests for connect_interface_to_switch() orphan-cable defensive pre-check.

SW-244: netbox-agent crashed with NetBox 400 "Duplicate termination" when
a switch-side interface already had a cable with only one side terminated
("orphan"). The fix detects this before calling cables.create().
"""

import sys
import logging
import pytest
from unittest.mock import MagicMock, patch, call
from types import SimpleNamespace


# ---------------------------------------------------------------------------
# Pre-mock netbox_agent.config to avoid import-time sys.argv parsing
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
    """
    Construct a ServerNetwork with all I/O mocked out.

    We bypass __init__ so we don't need a real 'server' object, then
    attach the minimal attributes that connect_interface_to_switch() uses.
    """
    from netbox_agent.network import ServerNetwork

    net = ServerNetwork.__new__(ServerNetwork)
    net.lldp = MagicMock()
    net.server = MagicMock()
    return net


def _make_nb_switch_interface(cable_obj):
    """Return a mock nb_switch_interface whose .cable attr is cable_obj."""
    iface = MagicMock()
    iface.id = 13261
    iface.cable = cable_obj
    return iface


class TestOrphanCableHandling:

    def setup_method(self):
        _mock_nb.reset_mock()

    def _setup_successful_lookup(self, nb_switch_interface, net):
        """
        Wire up the nb mocks so that connect_interface_to_switch() reaches
        the orphan-check block: valid IP → assigned object → device → interface.
        """
        switch_ip = "10.0.0.1"
        switch_interface_name = "swp36s2"

        mock_mgmt_ip = MagicMock()
        mock_mgmt_ip.assigned_object = MagicMock()
        mock_mgmt_ip.assigned_object.device = MagicMock()
        mock_mgmt_ip.assigned_object.device.id = 331

        _mock_nb.ipam.ip_addresses.get.return_value = mock_mgmt_ip
        _mock_nb.dcim.interfaces.get.return_value = nb_switch_interface
        net.lldp.get_switch_port.return_value = switch_interface_name

        nb_server_interface = MagicMock()
        nb_server_interface.name = "enp193s0f0np0"

        return switch_ip, switch_interface_name, nb_server_interface

    # ------------------------------------------------------------------
    # Test 1: orphan cable (one side missing) → delete + create fresh
    # ------------------------------------------------------------------
    def test_connect_interface_to_switch_deletes_orphan_cable(self):
        """
        If the switch interface has a cable ref with only b_terminations,
        the agent should delete the orphan and then create a new cable.
        """
        net = _make_server_network()

        # Stub cable reference on the switch interface
        cable_ref = MagicMock()
        cable_ref.id = 467
        nb_switch_interface = _make_nb_switch_interface(cable_ref)

        switch_ip, switch_iface_name, nb_server_interface = self._setup_successful_lookup(
            nb_switch_interface, net
        )

        # The orphan cable: a_terminations empty, b_terminations populated
        orphan_cable = MagicMock()
        orphan_cable.id = 467
        orphan_cable.a_terminations = []
        orphan_cable.b_terminations = [{"object_type": "dcim.interface", "object_id": 13261}]

        _mock_nb.dcim.cables.get.return_value = orphan_cable
        new_cable = MagicMock()
        _mock_nb.dcim.cables.create.return_value = new_cable

        with patch("netbox_agent.network.nb", _mock_nb):
            result = net.connect_interface_to_switch(
                switch_ip, switch_iface_name, nb_server_interface
            )

        # Orphan must be fetched and then deleted
        _mock_nb.dcim.cables.get.assert_called_once_with(467)
        orphan_cable.delete.assert_called_once()

        # A new fully-terminated cable must be created
        _mock_nb.dcim.cables.create.assert_called_once()
        create_kwargs = _mock_nb.dcim.cables.create.call_args
        assert create_kwargs is not None, "cables.create was not called"

        assert result is nb_server_interface

    # ------------------------------------------------------------------
    # Test 2: valid cable to a different endpoint → skip, no create
    # ------------------------------------------------------------------
    def test_connect_interface_to_switch_skips_when_switch_already_cabled_validly(self):
        """
        If the switch interface has a fully-terminated cable (both sides),
        the agent must NOT call cables.create (that would cause a NetBox 400).
        It should log an error and return nb_server_interface unchanged.
        """
        net = _make_server_network()

        cable_ref = MagicMock()
        cable_ref.id = 999
        nb_switch_interface = _make_nb_switch_interface(cable_ref)

        switch_ip, switch_iface_name, nb_server_interface = self._setup_successful_lookup(
            nb_switch_interface, net
        )

        # Both terminations present → valid cable
        valid_cable = MagicMock()
        valid_cable.id = 999
        valid_cable.a_terminations = [{"object_type": "dcim.interface", "object_id": 5000}]
        valid_cable.b_terminations = [{"object_type": "dcim.interface", "object_id": 13261}]

        _mock_nb.dcim.cables.get.return_value = valid_cable

        with patch("netbox_agent.network.nb", _mock_nb), \
             patch("netbox_agent.network.logging") as mock_logging:
            result = net.connect_interface_to_switch(
                switch_ip, switch_iface_name, nb_server_interface
            )

        # Must NOT create a new cable
        _mock_nb.dcim.cables.create.assert_not_called()

        # Must log an error
        mock_logging.error.assert_called()
        error_msg = mock_logging.error.call_args[0][0]
        assert "already cabled" in error_msg or "duplicate" in error_msg.lower() or \
               "fully-terminated" in error_msg or "manually" in error_msg

        # Must return the server interface unchanged
        assert result is nb_server_interface

    # ------------------------------------------------------------------
    # Test 3: no existing cable → normal create path (regression guard)
    # ------------------------------------------------------------------
    def test_connect_interface_to_switch_normal_path(self):
        """
        When nb_switch_interface.cable is None, the agent should call
        cables.create without touching cables.get or cables.delete.
        This is the happy-path regression guard.
        """
        net = _make_server_network()

        # No existing cable
        nb_switch_interface = _make_nb_switch_interface(None)

        switch_ip, switch_iface_name, nb_server_interface = self._setup_successful_lookup(
            nb_switch_interface, net
        )

        new_cable = MagicMock()
        _mock_nb.dcim.cables.create.return_value = new_cable

        with patch("netbox_agent.network.nb", _mock_nb):
            result = net.connect_interface_to_switch(
                switch_ip, switch_iface_name, nb_server_interface
            )

        # cables.get should NOT be called (no cable to inspect)
        _mock_nb.dcim.cables.get.assert_not_called()

        # cables.create MUST be called
        _mock_nb.dcim.cables.create.assert_called_once()

        assert result is nb_server_interface

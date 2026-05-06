"""Tests for LLDP._filter_to_switch_neighbors (INF-318).

Verifies that on multi-neighbor interfaces — e.g. a host iface that picks
up both the real switch's LLDP and a Smart NIC SoC's self-LLDP — only the
switch (Bridge/Router-capable) block survives so it isn't overwritten in
parse(). Without the filter, a Station block arriving last would silently
displace the switch entry, blanking the cable target.
"""

import sys
import types
from unittest.mock import MagicMock


_mock_misc = types.ModuleType("netbox_agent.misc")
_mock_misc.is_tool = lambda _: True
sys.modules["netbox_agent.misc"] = _mock_misc

from netbox_agent.lldp import LLDP  # noqa: E402


def _block(iface, rid, chassis_mac, mgmt_ip, port_key, port_val, capabilities):
    """Synthesize one keyvalue neighbor block for an iface.

    capabilities: dict mapping capability name -> 'on'/'off'.
    """
    lines = [
        f"lldp.{iface}.via=LLDP",
        f"lldp.{iface}.rid={rid}",
        f"lldp.{iface}.age=0 day, 00:00:01",
        f"lldp.{iface}.chassis.mac={chassis_mac}",
        f"lldp.{iface}.chassis.name=test-{rid}",
        f"lldp.{iface}.chassis.mgmt-ip={mgmt_ip}",
    ]
    for cap, state in capabilities.items():
        lines.append(f"lldp.{iface}.chassis.{cap}.enabled={state}")
    lines.append(f"lldp.{iface}.port.{port_key}={port_val}")
    return "\n".join(lines)


def test_filter_keeps_bridge_only_block():
    out = _block(
        "eth0", 1, "aa:bb:cc:dd:ee:ff", "10.0.0.1", "ifname", "swp1",
        {"Bridge": "on"},
    )
    lldp = LLDP(output=out)
    assert lldp.get_switch_ip("eth0") == "10.0.0.1"
    assert lldp.get_switch_port("eth0") == "swp1"


def test_filter_drops_station_only_block():
    out = _block(
        "enp1", 5, "00:1a:ca:ff:ff:01", "192.168.100.2", "mac", "02:f2:3b:62:b2:ca",
        {"Station": "on", "Bridge": "off", "Router": "off"},
    )
    lldp = LLDP(output=out)
    # iface should be entirely absent — no switch found
    assert lldp.get_switch_ip("enp1") is None


def test_filter_picks_switch_when_station_arrives_last():
    """The exact INF-318 repro: BF3 self-LLDP arrives after the real switch
    on the same iface. Without the filter, parse()'s overwrite makes BF3 win.
    With the filter, the Station block is discarded and the switch survives
    regardless of arrival order.
    """
    switch = _block(
        "enp193s0f0np0", 45, "c4:70:bd:f3:f8:dc", "10.100.198.24",
        "ifname", "swp34s0",
        {"Bridge": "on", "Router": "on"},
    )
    bf3_self = _block(
        "enp193s0f0np0", 6, "00:1a:ca:ff:ff:01", "192.168.100.2",
        "mac", "86:5c:f0:89:19:96",
        {"Station": "on", "Bridge": "off", "Router": "off"},
    )
    out = "\n".join([switch, bf3_self])  # BF3 last in stream
    lldp = LLDP(output=out)
    assert lldp.get_switch_ip("enp193s0f0np0") == "10.100.198.24"
    assert lldp.get_switch_port("enp193s0f0np0") == "swp34s0"


def test_filter_router_capability_also_kept():
    out = _block(
        "eth0", 1, "aa:bb:cc:dd:ee:ff", "10.0.0.2", "ifname", "swp2",
        {"Router": "on"},
    )
    lldp = LLDP(output=out)
    assert lldp.get_switch_ip("eth0") == "10.0.0.2"


def test_filter_no_capability_block_dropped():
    """Defensive: a block with no capability TLV is dropped (treated as
    not-a-switch). In practice all switches in our fleet advertise
    Bridge or Router; absence is suspicious."""
    out = _block(
        "eth0", 1, "aa:bb:cc:dd:ee:ff", "10.0.0.3", "ifname", "swp3",
        {},
    )
    lldp = LLDP(output=out)
    assert lldp.get_switch_ip("eth0") is None

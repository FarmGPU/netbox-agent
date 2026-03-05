"""
Tests for netbox_agent.ipmi — IPMI LAN channel parser.

No netbox_agent.config dependency, so no pre-mocking needed.
"""

from unittest.mock import patch

from netbox_agent.ipmi import IPMI, _CHANNELS


# Sample ipmitool output (real-world format)
_SAMPLE_OUTPUT = """\
Set in Progress         : Set Complete
Auth Type Support       :
IP Address Source       : DHCP Address
IP Address              : 10.192.2.1
Subnet Mask             : 255.255.240.0
MAC Address             : 98:f2:b3:f0:ee:1e
Default Gateway IP      : 10.192.2.254
802.1q VLAN ID          : Disabled
"""

_INVALID_IP_OUTPUT = """\
IP Address              : 0.0.0.0
Subnet Mask             : 0.0.0.0
MAC Address             : 00:00:00:00:00:00
802.1q VLAN ID          : Disabled
"""


class TestIPMI:

    def test_ipmi_channel_fallback(self):
        """Channel 1 fails (0.0.0.0), channel 2 succeeds."""
        call_count = [0]

        def mock_getstatusoutput(cmd):
            call_count[0] += 1
            if "lan print 1" in cmd:
                return (0, _INVALID_IP_OUTPUT)
            elif "lan print 2" in cmd:
                return (0, _SAMPLE_OUTPUT)
            return (1, "")

        with patch("netbox_agent.ipmi.which", return_value="/usr/bin/ipmitool"), \
             patch("netbox_agent.ipmi.subprocess.getstatusoutput",
                   side_effect=mock_getstatusoutput):
            ipmi = IPMI()

        assert ipmi.channel == 2
        result = ipmi.parse()
        assert result["ip"] == ["10.192.2.1/32"]
        assert result["mac"] == "98:F2:B3:F0:EE:1E"

    def test_ipmi_prefix_normalization_to_32(self):
        """OOB IP is always normalized to /32 regardless of subnet mask."""
        with patch("netbox_agent.ipmi.which", return_value="/usr/bin/ipmitool"), \
             patch("netbox_agent.ipmi.subprocess.getstatusoutput",
                   return_value=(0, _SAMPLE_OUTPUT)):
            ipmi = IPMI()
            result = ipmi.parse()

        assert result["ip"] == ["10.192.2.1/32"]
        # Old behavior would have been 10.192.0.0/20 from the /20 subnet mask

    def test_ipmi_unavailable_returns_empty(self):
        """No ipmitool on PATH → empty dict."""
        with patch("netbox_agent.ipmi.which", return_value=None):
            ipmi = IPMI()
            result = ipmi.parse()

        assert result == {}

    def test_ipmi_invalid_ip_skipped(self):
        """All channels return 0.0.0.0 → empty dict."""
        def mock_getstatusoutput(cmd):
            return (0, _INVALID_IP_OUTPUT)

        with patch("netbox_agent.ipmi.which", return_value="/usr/bin/ipmitool"), \
             patch("netbox_agent.ipmi.subprocess.getstatusoutput",
                   side_effect=mock_getstatusoutput):
            ipmi = IPMI()
            result = ipmi.parse()

        assert result == {}
        assert ipmi.channel is None

    def test_ipmi_vlan_parsing(self):
        """VLAN ID is parsed as int when not Disabled."""
        vlan_output = _SAMPLE_OUTPUT.replace(
            "802.1q VLAN ID          : Disabled",
            "802.1q VLAN ID          : 100",
        )
        with patch("netbox_agent.ipmi.which", return_value="/usr/bin/ipmitool"), \
             patch("netbox_agent.ipmi.subprocess.getstatusoutput",
                   return_value=(0, vlan_output)):
            ipmi = IPMI()
            result = ipmi.parse()

        assert result["vlan"] == 100

    def test_ipmi_all_channels_fail(self):
        """All channels return non-zero exit code → empty dict."""
        with patch("netbox_agent.ipmi.which", return_value="/usr/bin/ipmitool"), \
             patch("netbox_agent.ipmi.subprocess.getstatusoutput",
                   return_value=(1, "error")):
            ipmi = IPMI()
            result = ipmi.parse()

        assert result == {}

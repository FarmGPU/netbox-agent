"""
IPMI LAN channel parser.

Probes multiple BMC channels (1, 2, 8) and normalizes the OOB IP to /32.
Returns empty dict when ipmitool is missing or no valid IP is found.
"""

import logging
import subprocess
from shutil import which

logger = logging.getLogger("netbox_agent.ipmi")

# Channels to probe in order — most BMCs use 1, some use 2 or 8
_CHANNELS = [1, 2, 8]


class IPMI:
    """Parse IPMI LAN configuration from ipmitool."""

    def __init__(self):
        self.output = ""
        self.channel = None

        if not which("ipmitool"):
            logger.info("ipmitool not found — IPMI data unavailable")
            return

        for ch in _CHANNELS:
            ret, output = subprocess.getstatusoutput(
                f"ipmitool lan print {ch}"
            )
            if ret != 0:
                continue

            # Check for a real IP (not 0.0.0.0)
            ip = self._extract_field(output, "IP Address")
            if ip and ip != "0.0.0.0":
                self.output = output
                self.channel = ch
                logger.debug("IPMI: valid response on channel %d (IP=%s)", ch, ip)
                break
        else:
            logger.warning("IPMI: no valid response on channels %s", _CHANNELS)

    def parse(self):
        """Parse IPMI output into a network interface dict."""
        if not self.output:
            return {}

        fields = {}
        for line in self.output.splitlines():
            key = line.split(":")[0].strip()
            if key in ("802.1q VLAN ID", "IP Address", "Subnet Mask", "MAC Address"):
                value = ":".join(line.split(":")[1:]).strip()
                fields[key] = value

        try:
            mac = fields["MAC Address"]
            if mac:
                mac = mac.upper()
            vlan_raw = fields.get("802.1q VLAN ID", "Disabled")
            vlan = int(vlan_raw) if vlan_raw != "Disabled" else None
            ip = fields["IP Address"]
        except KeyError as e:
            logger.error("IPMI decoding failed, missing: %s", e.args[0])
            return {}

        if not ip or ip == "0.0.0.0":
            return {}

        # Normalize to /32 — BMC API uses /32 for OOB IPs
        address = f"{ip}/32"

        return {
            "name": "IPMI",
            "mac": mac,
            "ip": [address],
            "vlan": vlan,
            "mtu": 1500,
            "bonding": False,
            "ipmi": True,
        }

    @staticmethod
    def _extract_field(output, field_name):
        """Extract a single field value from ipmitool output."""
        for line in output.splitlines():
            key = line.split(":")[0].strip()
            if key == field_name:
                return ":".join(line.split(":")[1:]).strip()
        return None

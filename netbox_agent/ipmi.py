"""
IPMI LAN channel parser.

Probes multiple BMC channels (1, 2, 8) and normalizes the OOB IP to /32.
Returns interface dict with MAC even when IP is unassigned (0.0.0.0) so
the IPMI interface is visible in NetBox.  Returns empty dict only when
ipmitool is missing or no channel responds with a valid MAC.
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

            # Accept channel if it has a valid MAC (IP may be 0.0.0.0)
            mac = self._extract_field(output, "MAC Address")
            if mac and mac != "00:00:00:00:00:00":
                self.output = output
                self.channel = ch
                ip = self._extract_field(output, "IP Address") or "0.0.0.0"
                logger.debug(
                    "IPMI: valid response on channel %d (MAC=%s, IP=%s)", ch, mac, ip
                )
                break
        else:
            logger.warning("IPMI: no valid response on channels %s", _CHANNELS)

    def parse(self):
        """Parse IPMI output into a network interface dict.

        Returns interface dict with MAC always.  The ``ip`` list is empty
        when the BMC has no assigned IP (0.0.0.0), so the IPMI interface
        still appears in NetBox with its MAC for visibility.
        """
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
        except KeyError as e:
            logger.error("IPMI decoding failed, missing: %s", e.args[0])
            return {}

        # Build IP list — empty when BMC has no assigned IP
        ip = fields.get("IP Address", "")
        ip_list = []
        if ip and ip != "0.0.0.0":
            ip_list = [f"{ip}/32"]
        else:
            logger.info("IPMI: MAC=%s but IP unassigned (0.0.0.0) — interface created without IP", mac)

        return {
            "name": "IPMI",
            "mac": mac,
            "ip": ip_list,
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

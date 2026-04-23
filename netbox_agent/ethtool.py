import re
import subprocess
from shutil import which

from netbox_agent.config import config

#  Originally from https://github.com/opencoff/useful-scripts/blob/master/linktest.py

# mapping fields from ethtool output to simple names
field_map = {
    "Supported ports": "ports",
    "Supported link modes": "sup_link_modes",
    "Supports auto-negotiation": "sup_autoneg",
    "Advertised link modes": "adv_link_modes",
    "Advertised auto-negotiation": "adv_autoneg",
    "Speed": "speed",
    "Duplex": "duplex",
    "Port": "port",
    "Auto-negotiation": "autoneg",
    "Link detected": "link",
}

# mapping fields from ethtool -m (module/transceiver) output
module_field_map = {
    "Identifier": "transceiver_type",
    "Vendor name": "transceiver_vendor",
    "Vendor OUI": "transceiver_oui",
    "Vendor PN": "transceiver_part_number",
    "Vendor rev": "transceiver_revision",
    "Vendor SN": "transceiver_serial",
    "Date code": "transceiver_date_code",
    "Connector": "transceiver_connector",
    "Transmitter type": "transceiver_transmitter",
    "Encoding": "transceiver_encoding",
    "Extended identifier": "transceiver_extended_id",
    "Length (SMF,km)": "transceiver_length_smf_km",
    "Length (SMF)": "transceiver_length_smf",
    "Length (50um)": "transceiver_length_50um",
    "Length (62.5um)": "transceiver_length_625um",
    "Length (Copper or Active cable)": "transceiver_length_copper",
    "Length (OM3)": "transceiver_length_om3",
    "Length (OM4)": "transceiver_length_om4",
    "Module temperature": "transceiver_temperature",
    "Module voltage": "transceiver_voltage",
    "Wavelength": "transceiver_wavelength",
    "Laser wavelength": "transceiver_wavelength",
}


def merge_two_dicts(x, y):
    z = x.copy()
    z.update(y)
    return z


class Ethtool:
    """
    This class aims to parse ethtool output
    There is several bindings to have something proper, but it requires
    compilation and other requirements.
    """

    def __init__(self, interface, *args, **kwargs):
        self.interface = interface

    def _parse_ethtool_output(self):
        """
        parse ethtool output
        """

        output = subprocess.getoutput("ethtool {}".format(self.interface))

        fields = {
            "speed": "-",
            "max_speed": "-",
            "link": "-",
            "duplex": "-",
        }
        field = ""
        for line in output.split("\n")[1:]:
            line = line.rstrip()
            r = line.find(":")
            if r > 0:
                field = line[:r].strip()
                if field not in field_map:
                    continue
                field_key = field_map[field]
                output = line[r + 1 :].strip()
                fields[field_key] = output
            else:
                if len(field) > 0 and field in field_map:
                    field_key = field_map[field]
                    fields[field_key] += " " + line.strip()

        numbers = re.compile(r"\d+")
        supported_speeds = [
            int(match.group(0)) for match in numbers.finditer(fields.get("sup_link_modes", ""))
        ]
        if supported_speeds:
            fields["max_speed"] = "{}Mb/s".format(max(supported_speeds))

        for k in ("speed", "duplex"):
            if fields[k].startswith("Unknown!"):
                fields[k] = "-"

        return fields

    def _parse_ethtool_module_output(self):
        """Parse ethtool -m output for transceiver/module information.

        Extracts vendor, part number, serial, form factor, cable length,
        temperature, voltage, and wavelength from SFP/QSFP modules.
        """
        status, output = subprocess.getstatusoutput("ethtool -m {}".format(self.interface))
        if status != 0:
            return {}

        fields = {}

        # Extract form factor from Identifier line (legacy behavior)
        r = re.search(r"Identifier.*\((\w+)\)", output)
        if r and len(r.groups()) > 0:
            fields["form_factor"] = r.groups()[0]

        # Parse all module fields
        for line in output.split("\n"):
            line = line.strip()
            colon = line.find(":")
            if colon <= 0:
                continue
            key = line[:colon].strip()
            value = line[colon + 1:].strip()

            if key in module_field_map:
                field_key = module_field_map[key]
                # Clean up the value
                if field_key == "transceiver_type":
                    # Extract the human-readable part: "0x11 (QSFP28)" → "QSFP28"
                    m = re.search(r"\(([^)]+)\)", value)
                    fields[field_key] = m.group(1) if m else value
                elif field_key == "transceiver_vendor":
                    fields[field_key] = value.strip()
                elif field_key == "transceiver_temperature":
                    # "38.00 degrees C" → "38.00"
                    m = re.match(r"([\d.]+)", value)
                    fields[field_key] = m.group(1) if m else value
                elif field_key == "transceiver_voltage":
                    m = re.match(r"([\d.]+)", value)
                    fields[field_key] = m.group(1) if m else value
                else:
                    fields[field_key] = value

        return fields

    def parse_ethtool_mac_output(self):
        status, output = subprocess.getstatusoutput("ethtool -P {}".format(self.interface))
        if status == 0:
            match = re.search(r"[0-9a-f:]{17}", output)
            if match and match.group(0) != "00:00:00:00:00:00":
                return {"mac_address": match.group(0)}
        return {}

    def parse(self):
        if which("ethtool") is None:
            return None
        output = self._parse_ethtool_output()
        output.update(self._parse_ethtool_module_output())
        output.update(self.parse_ethtool_mac_output())
        return output

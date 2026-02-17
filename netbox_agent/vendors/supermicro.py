import logging
import re
import subprocess

from netbox_agent.location import Slot
from netbox_agent.misc import is_tool
from netbox_agent.server import ServerBase


class SupermicroHost(ServerBase):
    """
    Supermicro DMI can be messed up.  They depend on the vendor
    to set the correct values.  The endusers cannot
    change them without buying a license from Supermicro.

    There are 3 serial numbers in the system

      1) System - this is used for the chassis information.
      2) Baseboard - this is used for the blade.
      3) Chassis - this is ignored.

    """

    def __init__(self, *args, **kwargs):
        super(SupermicroHost, self).__init__(*args, **kwargs)
        self.manufacturer = "Supermicro"

    def is_blade(self):
        product_name = self.system[0]["Product Name"].strip()
        # Blades
        blade = product_name.startswith("SBI")
        blade |= product_name.startswith("SBA")
        # Twin
        blade |= "TR-" in product_name
        # TwinPro
        blade |= "TP-" in product_name
        # BigTwin
        blade |= "BT-" in product_name
        # Microcloud
        blade |= product_name.startswith("SYS-5039")
        blade |= product_name.startswith("SYS-5038")
        return blade

    def get_blade_slot(self):
        if self.is_blade():
            # Some Supermicro servers don't report the slot in dmidecode
            # let's use a regex
            slot = Slot()
            return slot.get()
        # No supermicro on hands
        return None

    def get_service_tag(self):
        default_serial = "0123456789"
        baseboard_serial = self.baseboard[0]["Serial Number"].strip()
        system_serial = str(self.system[0]["Serial Number"]).strip()

        if self.is_blade() or system_serial == default_serial:
            return baseboard_serial
        return system_serial

    def get_product_name(self):
        if self.is_blade():
            return self.baseboard[0]["Product Name"].strip()
        return self.system[0]["Product Name"].strip()

    def get_chassis(self):
        if self.is_blade():
            return self.system[0]["Product Name"].strip()
        return self.get_product_name()

    def get_chassis_service_tag(self):
        if self.is_blade():
            return self.system[0]["Serial Number"].strip()
        return self.get_service_tag()

    def get_chassis_name(self):
        if not self.is_blade():
            return None
        return "Chassis {}".format(self.get_chassis_service_tag())

    # Default voltage for converting watts → amps when no per-PSU voltage
    # sensor is available.  Matches the 230V default in power.py so the
    # round-trip (amps * voltage → allocated_draw) is consistent.
    _DEFAULT_VOLTAGE = 230.0

    def get_power_consumption(self):
        """
        Read per-PSU amperage from IPMI sensors.

        Three strategies, tried in order:
          1. Per-PSU current sensors (Amps) from ``ipmitool sensor``
          2. Per-PSU power (Watts) + voltage from ``ipmitool sensor``
          3. Total system power from ``ipmitool dcmi power reading``,
             split evenly across present PSUs

        Returns a list of amperage strings, one per PSU — matching the
        interface used by DellHost.get_power_consumption() and consumed
        by PowerSupply.report_power_consumption().
        """
        if not is_tool("ipmitool"):
            logging.error("ipmitool not found, cannot read power consumption")
            return []

        data = subprocess.getoutput("ipmitool sensor")

        # Parse sensor readings: name -> (value, unit)
        sensors = {}
        for line in data.splitlines():
            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 3:
                continue
            name, reading, unit = parts[0], parts[1], parts[2]
            try:
                value = float(reading)
            except (ValueError, TypeError):
                continue
            sensors[name] = (value, unit)

        # Strategy 1: Direct per-PSU current readings (Amps)
        psu_amps = {}
        for name, (value, unit) in sensors.items():
            if "Amps" not in unit:
                continue
            match = re.search(r"PSU?\s*(\d+)", name, re.IGNORECASE)
            if match:
                psu_num = int(match.group(1))
                psu_amps[psu_num] = value

        if psu_amps:
            logging.debug(
                "SuperMicro power: found current sensors for %d PSU(s)", len(psu_amps)
            )
            return [str(psu_amps[k]) for k in sorted(psu_amps)]

        # Strategy 2: Compute amps from per-PSU power (Watts) / voltage (Volts)
        psu_watts = {}
        psu_volts = {}
        for name, (value, unit) in sensors.items():
            match = re.search(r"PSU?\s*(\d+)", name, re.IGNORECASE)
            if not match:
                continue
            psu_num = int(match.group(1))
            if "Watts" in unit:
                psu_watts[psu_num] = value
            elif "Volts" in unit:
                psu_volts[psu_num] = value

        if psu_watts:
            result = []
            for psu_num in sorted(psu_watts):
                watts = psu_watts[psu_num]
                volts = psu_volts.get(psu_num, self._DEFAULT_VOLTAGE)
                amps = watts / volts if volts > 0 else 0
                result.append(str(round(amps, 3)))
            logging.debug(
                "SuperMicro power: computed amps from watts for %d PSU(s)", len(result)
            )
            return result

        # Strategy 3: DCMI total system power, split across present PSUs
        return self._get_power_from_dcmi()

    def _get_power_from_dcmi(self):
        """
        Fallback: read total system watts from DCMI and split evenly
        across detected PSUs.  Many SuperMicro models (e.g. SYS-212H-TN)
        lack per-PSU power sensors but support DCMI power reading.
        """
        try:
            dcmi_out = subprocess.getoutput("ipmitool dcmi power reading")
            total_watts = None
            for line in dcmi_out.splitlines():
                if "Instantaneous power reading" in line:
                    match = re.search(r"(\d+)\s*Watts", line)
                    if match:
                        total_watts = float(match.group(1))
                        break
            if total_watts is None or total_watts <= 0:
                logging.warning("No valid DCMI power reading found")
                return []
        except Exception as e:
            logging.warning("DCMI power reading failed: %s", e)
            return []

        # Count present PSUs from SDR
        psu_count = self._count_present_psus()
        if psu_count < 1:
            psu_count = 1

        per_psu_watts = total_watts / psu_count
        per_psu_amps = per_psu_watts / self._DEFAULT_VOLTAGE

        logging.debug(
            "SuperMicro power: DCMI total %dW / %d PSU(s) = %.1fW each (%.3fA @ %dV)",
            total_watts, psu_count, per_psu_watts, per_psu_amps,
            int(self._DEFAULT_VOLTAGE),
        )
        return [str(round(per_psu_amps, 3))] * psu_count

    def _count_present_psus(self):
        """Count PSUs reporting 'Presence detected' in SDR."""
        try:
            sdr_out = subprocess.getoutput('ipmitool sdr type "Power Supply"')
            count = sum(
                1 for line in sdr_out.splitlines()
                if "Presence detected" in line
            )
            return count if count > 0 else 1
        except Exception:
            return 1

    def get_expansion_product(self):
        """
        Get the extension slot that is on a pair slot number
        next to the compute slot that is on an odd slot number
        I only know on model of slot GPU extension card that.
        """
        raise NotImplementedError

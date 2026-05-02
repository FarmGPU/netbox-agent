import logging
import os
import re
from itertools import chain, islice
from pathlib import Path

import netifaces
from netaddr import IPAddress
from packaging import version

from netbox_agent.config import config
from netbox_agent.config import netbox_instance as nb
from netbox_agent.ethtool import Ethtool
from netbox_agent.ipmi import IPMI
from netbox_agent.lldp import LLDP

VIRTUAL_NET_FOLDER = Path("/sys/devices/virtual/net")


# Per-device cache: interface MAC (upper) → nic_module pynetbox object
_nic_module_cache = {}


def _clear_nic_module_cache():
    """Reset the NIC module cache.  Call once at the start of each device sync."""
    global _nic_module_cache
    _nic_module_cache = {}


def _find_or_create_manufacturer(vendor):
    """Return a pynetbox Manufacturer for *vendor*, creating if needed."""
    mfr_slug = re.sub(r"[^a-z0-9-]", "", vendor.lower().replace(" ", "-"))[:50]
    mfr = nb.dcim.manufacturers.get(slug=mfr_slug)
    if not mfr:
        mfr = nb.dcim.manufacturers.get(name=vendor)
    if not mfr:
        mfr = nb.dcim.manufacturers.create(name=vendor, slug=mfr_slug)
        logging.info("Created manufacturer: %s", vendor)
    return mfr


def _find_nic_module_for_interface(device_id, interface):
    """Find the NIC Module that owns a given interface.

    The module sync (``modules.py``) creates per-port NIC modules in bays
    named ``NIC-0``, ``NIC-1``, etc. with the interface MAC as the module
    serial.  This function looks up that module by matching the interface's
    MAC address.

    Returns the pynetbox Module object, or None.
    """
    global _nic_module_cache

    mac = getattr(interface, "mac_address", None)
    if not mac:
        return None
    mac_upper = str(mac).upper()

    if mac_upper in _nic_module_cache:
        return _nic_module_cache[mac_upper]

    # Build cache on first miss: load all NIC-* bays for this device
    if not _nic_module_cache.get("_loaded_{}".format(device_id)):
        all_bays = list(nb.dcim.module_bays.filter(device_id=device_id))
        for bay in all_bays:
            if not bay.name.startswith("NIC-"):
                continue
            modules = list(nb.dcim.modules.filter(module_bay_id=bay.id))
            for mod in modules:
                if mod.serial:
                    _nic_module_cache[mod.serial.upper()] = mod
        _nic_module_cache["_loaded_{}".format(device_id)] = True

    return _nic_module_cache.get(mac_upper)


def _sync_transceiver_module(device_id, interface, ethtool_data):
    """Create or update a transceiver Module as a child of its NIC module.

    The module sync (``modules.py``) creates per-port NIC modules in bays
    ``NIC-0``, ``NIC-1``, etc.  This function adds a child ``XCVR-0`` bay
    to the NIC module and installs the transceiver there::

        Device
          └─ ModuleBay  "NIC-2"
               └─ Module  ConnectX-7          ← created by modules.py
                    └─ ModuleBay  "XCVR-0"
                         └─ Module  T1Q112    ← created here

    Falls back to a device-level ``<iface>-xcvr`` bay when no NIC module is
    found (e.g. because modules.py hasn't run yet or the NIC is virtual).

    Args:
        device_id: NetBox device ID
        interface: pynetbox interface object (already saved)
        ethtool_data: dict from Ethtool.parse() with transceiver_* fields
    """
    if not ethtool_data or not isinstance(ethtool_data, dict):
        return

    vendor = (ethtool_data.get("transceiver_vendor") or "").strip()
    part_number = (ethtool_data.get("transceiver_part_number") or "").strip()
    serial = (ethtool_data.get("transceiver_serial") or "").strip()
    form_factor = (ethtool_data.get("transceiver_type") or
                   ethtool_data.get("form_factor") or "").strip()

    # Need at least vendor or part number to create a module type
    if not vendor and not part_number:
        return

    model = part_number or form_factor or "Unknown Transceiver"
    if not vendor:
        vendor = "Unknown"

    try:
        # --- Find parent NIC module (created by modules.py) ---
        nic_module = _find_nic_module_for_interface(device_id, interface)

        # Link interface to its NIC module
        if nic_module:
            current_mod = getattr(interface, "module", None)
            current_mod_id = current_mod.id if hasattr(current_mod, "id") else current_mod
            if current_mod_id != nic_module.id:
                interface.module = nic_module.id
                interface.save()

        # --- Transceiver manufacturer ---
        mfr = _find_or_create_manufacturer(vendor)

        # --- Transceiver ModuleType ---
        module_type = None
        if part_number:
            existing = list(nb.dcim.module_types.filter(
                part_number=part_number, manufacturer_id=mfr.id))
            if existing:
                module_type = existing[0]
        if not module_type:
            existing = list(nb.dcim.module_types.filter(
                model=model, manufacturer_id=mfr.id))
            if existing:
                module_type = existing[0]
        if not module_type:
            module_type = nb.dcim.module_types.create(
                manufacturer=mfr.id,
                model=model,
                part_number=part_number,
            )
            logging.info("Created transceiver module type: %s %s", vendor, model)

        # --- XCVR ModuleBay (child of NIC module, or device-level fallback) ---
        if nic_module:
            # Each per-port NIC module gets one XCVR child bay
            xcvr_bay_name = "XCVR-0"
            xcvr_bays = list(nb.dcim.module_bays.filter(
                module_id=nic_module.id, name=xcvr_bay_name))
            if xcvr_bays:
                bay = xcvr_bays[0]
            else:
                # NetBox requires device even for module-level bays
                bay = nb.dcim.module_bays.create(
                    device=device_id, module=nic_module.id,
                    name=xcvr_bay_name)
                logging.info("Created XCVR bay: %s on NIC module %s (id=%s)",
                             xcvr_bay_name, nic_module.module_type, nic_module.id)

            # Clean up legacy device-level fallback bay if it exists
            legacy_bay_name = "%s-xcvr" % interface.name
            legacy_bays = list(nb.dcim.module_bays.filter(
                device_id=device_id, name=legacy_bay_name))
            for lb in legacy_bays:
                # Migrate any module from legacy bay to proper XCVR bay
                legacy_mods = list(nb.dcim.modules.filter(module_bay_id=lb.id))
                for lm in legacy_mods:
                    logging.info(
                        "Migrating transceiver from legacy bay '%s' to '%s' on NIC module",
                        legacy_bay_name, xcvr_bay_name,
                    )
                    lm.module_bay = bay.id
                    lm.save()
                lb.delete()
                logging.info("Deleted legacy fallback bay '%s'", legacy_bay_name)
        else:
            # No NIC module found — skip creating device-level fallback bays.
            # The module sync (modules.py) should create NIC modules first.
            # Transceiver will be picked up on the next run once NIC modules exist.
            logging.debug(
                "No NIC module found for interface '%s' — skipping transceiver bay creation",
                interface.name,
            )
            return

        # --- Transceiver Module ---
        existing_modules = list(nb.dcim.modules.filter(module_bay_id=bay.id))
        if existing_modules:
            module = existing_modules[0]
            dirty = False
            if serial and module.serial != serial:
                module.serial = serial
                dirty = True
            if module.module_type.id != module_type.id:
                module.module_type = module_type.id
                dirty = True
            if dirty:
                module.save()
                logging.info("Updated transceiver: %s %s (SN:%s) on %s",
                             vendor, model, serial, interface.name)
            return

        # Check by serial — optic may have moved bays
        if serial:
            by_sn = list(nb.dcim.modules.filter(
                serial=serial, device_id=device_id))
            if by_sn:
                module = by_sn[0]
                module.module_bay = bay.id
                module.module_type = module_type.id
                module.save()
                logging.info("Moved transceiver SN:%s → %s", serial, bay.name)
                return

        # Create new transceiver module
        nb.dcim.modules.create(
            device=device_id,
            module_bay=bay.id,
            module_type=module_type.id,
            serial=serial or "",
            custom_fields={"owner": "FarmGPU"},
        )
        logging.info(
            "Created transceiver: %s %s (SN:%s) on %s",
            vendor, model, serial, interface.name,
        )

    except Exception:
        logging.debug(
            "Failed to sync transceiver for %s", interface.name,
            exc_info=True,
        )


def _build_transceiver_description(ethtool_data):
    """Build a human-readable transceiver description from ethtool module data.

    Returns a string like "QSFP28 | Mellanox MCP1600-C003E30N (SN: MT2117VS05677) | 3m copper"
    or None if no transceiver data is available.
    """
    if not ethtool_data or not isinstance(ethtool_data, dict):
        return None

    parts = []

    # Form factor (QSFP28, SFP+, etc.)
    form = ethtool_data.get("transceiver_type") or ethtool_data.get("form_factor")
    if form:
        parts.append(form)

    # Vendor + part number
    vendor = ethtool_data.get("transceiver_vendor", "").strip()
    pn = ethtool_data.get("transceiver_part_number", "").strip()
    sn = ethtool_data.get("transceiver_serial", "").strip()
    if vendor or pn:
        vendor_str = "%s %s" % (vendor, pn) if vendor and pn else (vendor or pn)
        if sn:
            vendor_str += " (SN: %s)" % sn
        parts.append(vendor_str)

    # Cable length
    for length_key in ("transceiver_length_copper", "transceiver_length_om3",
                       "transceiver_length_om4", "transceiver_length_smf"):
        length = ethtool_data.get(length_key, "").strip()
        if length and length != "0m" and length != "0":
            connector = ethtool_data.get("transceiver_connector", "").strip()
            transmitter = ethtool_data.get("transceiver_transmitter", "").strip()
            if "copper" in (connector + transmitter).lower():
                parts.append("%s copper" % length)
            else:
                parts.append("%s fiber" % length)
            break

    # Wavelength (for fiber optics)
    wavelength = ethtool_data.get("transceiver_wavelength", "").strip()
    if wavelength and "nm" in wavelength:
        parts.append(wavelength)

    if not parts:
        return None

    return " | ".join(parts)


class Network(object):
    def __init__(self, server, *args, **kwargs):
        self.nics = []

        self.server = server
        self.tenant = self.server.get_netbox_tenant()

        self.lldp = LLDP() if config.network.lldp else None
        self.nics = self.scan()
        self.ipmi = None
        self.dcim_choices = {}
        dcim_c = nb.dcim.interfaces.choices()
        for _choice_type in dcim_c:
            key = "interface:{}".format(_choice_type)
            self.dcim_choices[key] = {}
            for choice in dcim_c[_choice_type]:
                self.dcim_choices[key][choice["display_name"]] = choice["value"]

        self.ipam_choices = {}
        ipam_c = nb.ipam.ip_addresses.choices()
        for _choice_type in ipam_c:
            key = "ip-address:{}".format(_choice_type)
            self.ipam_choices[key] = {}
            for choice in ipam_c[_choice_type]:
                self.ipam_choices[key][choice["display_name"]] = choice["value"]

    def get_network_type():
        return NotImplementedError

    # Proxmox VE creates many virtual bridge/firewall/tap interfaces that
    # clutter NetBox.  When /etc/pve/ exists (present on every PVE node)
    # these patterns are automatically appended to the configured
    # ignore_interfaces regex — no manual config change required.
    _PROXMOX_IFACE_PATTERNS = r"(fwbr.*|fwln.*|fwpr.*|tap\d+i\d+|vmbr\d+|ovs.*)"

    @staticmethod
    def _build_ignore_re():
        """Return the compiled ignore regex, extending it for Proxmox hosts."""
        base = config.network.ignore_interfaces or ""
        if os.path.isdir("/etc/pve"):
            if base:
                base = f"{base}|{ServerNetwork._PROXMOX_IFACE_PATTERNS}"
            else:
                base = ServerNetwork._PROXMOX_IFACE_PATTERNS
            logging.debug("Proxmox detected — extended ignore pattern: %s", base)
        return re.compile(base) if base else None

    def scan(self):
        nics = []
        ignore_re = self._build_ignore_re()
        for interface in os.listdir("/sys/class/net/"):
            # ignore if it's not a link (ie: bonding_masters etc)
            if not os.path.islink("/sys/class/net/{}".format(interface)):
                continue

            if ignore_re and ignore_re.match(interface):
                logging.debug("Ignore interface {interface}".format(interface=interface))
                continue

            ip_addr = netifaces.ifaddresses(interface).get(netifaces.AF_INET, [])
            ip6_addr = netifaces.ifaddresses(interface).get(netifaces.AF_INET6, [])
            if config.network.ignore_ips:
                ip_addr = [ip for ip in ip_addr
                           if not re.match(config.network.ignore_ips, ip["addr"])]
                ip6_addr = [ip for ip in ip6_addr
                            if not re.match(config.network.ignore_ips, ip["addr"])]

            # netifaces returns a ipv6 netmask that netaddr does not understand.
            # this strips the netmask down to the correct format for netaddr,
            # and remove the interface.
            # ie, this:
            #   {
            #      'addr': 'fe80::ec4:7aff:fe59:ec4a%eno1.50',
            #      'netmask': 'ffff:ffff:ffff:ffff::/64'
            #   }
            #
            # becomes:
            #   {
            #      'addr': 'fe80::ec4:7aff:fe59:ec4a',
            #      'netmask': 'ffff:ffff:ffff:ffff::'
            #   }
            #
            for addr in ip6_addr:
                addr["addr"] = addr["addr"].replace("%{}".format(interface), "")
                addr["mask"] = addr["mask"].split("/")[0]
                ip_addr.append(addr)

            ethtool = Ethtool(interface).parse()
            if (
                config.network.primary_mac == "permanent"
                and ethtool
                and ethtool.get("mac_address")
            ):
                mac = ethtool["mac_address"]
            else:
                mac = open("/sys/class/net/{}/address".format(interface), "r").read().strip()
                if mac == "00:00:00:00:00:00":
                    mac = None
            if mac:
                mac = mac.upper()
                # Filter out InfiniBand GUIDs (20 bytes) — only accept Ethernet MACs (6 bytes)
                # Valid Ethernet MAC: XX:XX:XX:XX:XX:XX = 17 chars
                if len(mac) != 17:
                    logging.debug(
                        "Skipping non-Ethernet MAC on %s: %s (%d chars)",
                        interface, mac, len(mac),
                    )
                    mac = None

            mtu = int(open("/sys/class/net/{}/mtu".format(interface), "r").read().strip())
            vlan = None
            if len(interface.split(".")) > 1:
                vlan = int(interface.split(".")[1])

            bonding = False
            bonding_slaves = []
            if os.path.isdir("/sys/class/net/{}/bonding".format(interface)):
                bonding = True
                bonding_slaves = (
                    open("/sys/class/net/{}/bonding/slaves".format(interface)).read().split()
                )

            virtual = Path(f"/sys/class/net/{interface}").resolve().parent == VIRTUAL_NET_FOLDER

            nic = {
                "name": interface,
                "mac": mac,
                "ip": [
                    "{}/{}".format(x["addr"], IPAddress(x["mask"]).netmask_bits())
                    for x in ip_addr
                    if "addr" in x and "mask" in x
                ]
                or None,  # FIXME: handle IPv6 addresses
                "ethtool": ethtool,
                "virtual": virtual,
                "vlan": vlan,
                "mtu": mtu,
                "bonding": bonding,
                "bonding_slaves": bonding_slaves,
            }
            nics.append(nic)
        return nics

    def _set_bonding_interfaces(self):
        bonding_nics = (x for x in self.nics if x["bonding"])
        for nic in bonding_nics:
            bond_int = self.get_netbox_network_card(nic)
            logging.debug("Setting slave interface for {name}".format(name=bond_int.name))
            for slave_int in (
                self.get_netbox_network_card(slave_nic)
                for slave_nic in self.nics
                if slave_nic["name"] in nic["bonding_slaves"]
            ):
                if slave_int.lag is None or slave_int.lag.id != bond_int.id:
                    logging.debug(
                        "Settting interface {name} as slave of {master}".format(
                            name=slave_int.name, master=bond_int.name
                        )
                    )
                    slave_int.lag = bond_int
                    slave_int.save()
        else:
            return False
        return True

    def get_network_cards(self):
        return self.nics

    def get_netbox_network_card(self, nic):
        if config.network.nic_id == "mac" and nic["mac"]:
            interface = self.nb_net.interfaces.get(mac_address=nic["mac"], **self.custom_arg_id)
        else:
            interface = self.nb_net.interfaces.get(name=nic["name"], **self.custom_arg_id)
        return interface

    def get_netbox_network_cards(self):
        return self.nb_net.interfaces.filter(**self.custom_arg_id)

    def get_netbox_type_for_nic(self, nic):
        if self.get_network_type() == "virtual":
            return self.dcim_choices["interface:type"]["Virtual"]

        if nic.get("bonding"):
            return self.dcim_choices["interface:type"]["Link Aggregation Group (LAG)"]

        if nic.get("virtual"):
            return self.dcim_choices["interface:type"]["Virtual"]

        if nic.get("ethtool") is None:
            return self.dcim_choices["interface:type"]["Other"]

        max_speed = nic["ethtool"]["max_speed"]
        if max_speed == "-":
            max_speed = nic["ethtool"]["speed"]

        if max_speed == "10000Mb/s":
            if nic["ethtool"]["port"] in ("FIBRE", "Direct Attach Copper"):
                return self.dcim_choices["interface:type"]["SFP+ (10GE)"]
            return self.dcim_choices["interface:type"]["10GBASE-T (10GE)"]

        elif max_speed == "25000Mb/s":
            if nic["ethtool"]["port"] in ("FIBRE", "Direct Attach Copper"):
                return self.dcim_choices["interface:type"]["SFP28 (25GE)"]

        elif max_speed == "5000Mb/s":
            return self.dcim_choices["interface:type"]["5GBASE-T (5GE)"]

        elif max_speed == "2500Mb/s":
            return self.dcim_choices["interface:type"]["2.5GBASE-T (2.5GE)"]

        elif max_speed == "1000Mb/s":
            if nic["ethtool"]["port"] in ("FIBRE", "Direct Attach Copper"):
                return self.dcim_choices["interface:type"]["SFP (1GE)"]
            return self.dcim_choices["interface:type"]["1000BASE-T (1GE)"]

        return self.dcim_choices["interface:type"]["Other"]

    def get_or_create_vlan(self, vlan_id):
        # FIXME: we may need to specify the datacenter
        # since users may have same vlan id in multiple dc
        vlan = nb.ipam.vlans.get(
            vid=vlan_id,
        )
        if vlan is None:
            vlan = nb.ipam.vlans.create(
                name="VLAN {}".format(vlan_id),
                vid=vlan_id,
            )
        return vlan

    def reset_vlan_on_interface(self, nic, interface):
        update = False
        vlan_id = nic["vlan"]
        lldp_vlan = (
            self.lldp.get_switch_vlan(nic["name"])
            if config.network.lldp and isinstance(self, ServerNetwork)
            else None
        )
        # For strange reason, we need to get the object from scratch
        # The object returned by pynetbox's save isn't always working (since pynetbox 6)
        interface = self.nb_net.interfaces.get(id=interface.id)

        # Handle the case were the local interface isn't an interface vlan as reported by Netbox
        # and that LLDP doesn't report a vlan-id
        if (
            vlan_id is None
            and lldp_vlan is None
            and (interface.mode is not None or len(interface.tagged_vlans) > 0)
        ):
            logging.info(
                "Interface {interface} is not tagged, reseting mode".format(interface=interface)
            )
            update = True
            interface.mode = None
            interface.tagged_vlans = []
            interface.untagged_vlan = None
        # if the local interface is configured with a vlan, it's supposed to be taggued
        # if mode is either not set or not correctly configured or vlan are not
        # correctly configured, we reset the vlan
        elif vlan_id and (
            interface.mode is None
            or type(interface.mode) is not int
            and (
                hasattr(interface.mode, "value")
                and interface.mode.value == self.dcim_choices["interface:mode"]["Access"]
                or len(interface.tagged_vlans) != 1
                or int(interface.tagged_vlans[0].vid) != int(vlan_id)
            )
        ):
            logging.info(
                "Resetting tagged VLAN(s) on interface {interface}".format(interface=interface)
            )
            update = True
            nb_vlan = self.get_or_create_vlan(vlan_id)
            interface.mode = self.dcim_choices["interface:mode"]["Tagged"]
            interface.tagged_vlans = [nb_vlan] if nb_vlan else []
            interface.untagged_vlan = None
        # Finally if LLDP reports a vlan-id with the pvid attribute
        elif lldp_vlan:
            pvid_vlan = [
                key for (key, value) in lldp_vlan.items() if "pvid" in value and value["pvid"]
            ]
            if len(pvid_vlan) > 0 and (
                interface.mode is None
                or interface.mode.value != self.dcim_choices["interface:mode"]["Access"]
                or interface.untagged_vlan is None
                or interface.untagged_vlan.vid != int(pvid_vlan[0])
            ):
                logging.info(
                    "Resetting access VLAN on interface {interface}".format(interface=interface)
                )
                update = True
                nb_vlan = self.get_or_create_vlan(pvid_vlan[0])
                interface.mode = self.dcim_choices["interface:mode"]["Access"]
                interface.untagged_vlan = nb_vlan.id
        return update, interface

    def _is_valid_mac(self, mac):
        """Check if MAC address is a valid 6-byte format (not an IB GUID)."""
        if not mac:
            return False
        # Standard MAC is 6 octets (17 chars with colons: AA:BB:CC:DD:EE:FF)
        # InfiniBand GUIDs are 20 octets which NetBox doesn't accept
        parts = mac.split(":")
        return len(parts) == 6 and all(len(p) == 2 for p in parts)

    def _all_macs(self, nic):
        """All MACs to sync onto a NIC: primary first, then permanent if distinct.

        For LACP bond slaves, `nic["mac"]` is the inherited bond MAC (shared
        across slaves) and `ethtool -P` returns the slave's hardware-burned
        MAC. Persisting both lets switch-side LACP partner-MAC observations
        resolve to the specific physical slave (INF-318).
        """
        macs = []
        primary = nic.get("mac")
        if primary and self._is_valid_mac(primary):
            macs.append(primary.upper())
        perm = (nic.get("ethtool") or {}).get("mac_address")
        if perm and self._is_valid_mac(perm):
            perm_u = perm.upper()
            if perm_u not in macs:
                macs.append(perm_u)
        return macs

    def update_interface_macs(self, nic, macs):
        """Sync MAC address objects on an interface. Returns current MAC objects."""
        nb_macs = list(self.nb_net.mac_addresses.filter(interface_id=nic.id))
        # Clean
        for nb_mac in nb_macs:
            if nb_mac.mac_address not in macs:
                logging.debug("Deleting extra MAC {mac} from {nic}".format(mac=nb_mac, nic=nic))
                nb_mac.delete()
        # Add missing
        for mac in macs:
            # Skip invalid MAC formats (e.g., InfiniBand GUIDs)
            if not self._is_valid_mac(mac):
                logging.debug("Skipping invalid MAC format {mac} on {nic}".format(mac=mac, nic=nic))
                continue
            if mac not in {nb_mac.mac_address for nb_mac in nb_macs}:
                logging.debug("Adding MAC {mac} to {nic}".format(mac=mac, nic=nic))
                self.nb_net.mac_addresses.create(
                    {
                        "mac_address": mac,
                        "assigned_object_type": "dcim.interface",
                        "assigned_object_id": nic.id,
                    }
                )
        # Return current state for primary_mac_address assignment
        return list(self.nb_net.mac_addresses.filter(interface_id=nic.id))

    def create_netbox_nic(self, nic, mgmt=False):
        nic_type = self.get_netbox_type_for_nic(nic)
        logging.info(
            "Creating NIC {name} ({mac}) on {device}".format(
                name=nic["name"], mac=nic["mac"], device=self.device.name
            )
        )

        nb_vlan = None

        params = dict(self.custom_arg)
        params.update(
            {
                "name": nic["name"],
                "type": nic_type,
                "mgmt_only": mgmt,
                "custom_fields": {"managed_by": "netbox-agent"},
            }
        )
        if nic["mac"] and len(nic["mac"]) == 17:
            params["mac_address"] = nic["mac"]

        if nic["mtu"]:
            params["mtu"] = nic["mtu"]

        if nic.get("ethtool") and nic["ethtool"].get("link") == "no":
            params["enabled"] = False

        # Add transceiver info to description if available
        transceiver_desc = _build_transceiver_description(nic.get("ethtool"))
        if transceiver_desc:
            params["description"] = transceiver_desc

        interface = self.nb_net.interfaces.create(**params)

        if nic["vlan"]:
            nb_vlan = self.get_or_create_vlan(nic["vlan"])
            interface.mode = self.dcim_choices["interface:mode"]["Tagged"]
            interface.tagged_vlans = [nb_vlan.id]
            interface.save()
        elif config.network.lldp and self.lldp.get_switch_vlan(nic["name"]) is not None:
            # if lldp reports a vlan on an interface, tag the interface in access and set the vlan
            # report only the interface which has `pvid=yes` (ie: lldp.eth3.vlan.pvid=yes)
            # if pvid is not present, it'll be processed as a vlan tagged interface
            vlans = self.lldp.get_switch_vlan(nic["name"])
            for vid, vlan_infos in vlans.items():
                nb_vlan = self.get_or_create_vlan(vid)
                if vlan_infos.get("vid"):
                    interface.mode = self.dcim_choices["interface:mode"]["Access"]
                    interface.untagged_vlan = nb_vlan.id
            interface.save()

        # cable the interface — but never on a bond/LAG iface itself.
        # NetBox 4.x rejects cables on type=lag; cables belong on the
        # physical slave NICs, which iterate as separate nics in the
        # outer loop (INF-320).
        if (
            config.network.lldp
            and isinstance(self, ServerNetwork)
            and not nic.get("bonding")
        ):
            switch_ip = self.lldp.get_switch_ip(interface.name)
            switch_interface = self.lldp.get_switch_port(interface.name)

            if switch_ip and switch_interface:
                nic_update, interface = self.create_or_update_cable(
                    switch_ip, switch_interface, interface
                )
                if nic_update:
                    interface.save()

        # Create transceiver Module if ethtool reports module info
        if not isinstance(self, VirtualNetwork) and nic.get("ethtool"):
            _sync_transceiver_module(self.device.id, interface, nic["ethtool"])

        return interface

    def create_or_update_netbox_ip_on_interface(self, ip, interface):
        """
        Two behaviors:
        - Anycast IP
        * If IP exists and is in Anycast, create a new Anycast one
        * If IP exists and isn't assigned, take it
        * If server is decomissioned, then free IP will be taken

        - Normal IP (can be associated only once)
        * If IP doesn't exist, create it
        * If IP exists and isn't assigned, take it
        * If IP exists and interface is wrong, change interface
        """
        netbox_ips = nb.ipam.ip_addresses.filter(
            address=ip,
        )
        # Also search by bare IP (without prefix) — BMC API may have stored
        # the same IP with a different prefix length (e.g., /32 vs /20)
        if not netbox_ips:
            bare_ip = ip.split("/")[0]
            netbox_ips = nb.ipam.ip_addresses.filter(address=bare_ip)

        if not netbox_ips:
            logging.info("Create new IP {ip} on {interface}".format(ip=ip, interface=interface))
            query_params = {
                "address": ip,
                "status": "active",
                "assigned_object_type": self.assigned_object_type,
                "assigned_object_id": interface.id,
                "dns_name": self._ip_dns_name(),
            }
            if self.tenant:
                query_params["tenant"] = self.tenant.id
            try:
                netbox_ip = nb.ipam.ip_addresses.create(**query_params)
            except Exception as e:
                # Handle race condition: IP was created between our filter and create
                if "Duplicate" in str(e):
                    logging.warning("Duplicate IP %s detected, finding existing entry", ip)
                    bare_ip = ip.split("/")[0]
                    netbox_ips = list(nb.ipam.ip_addresses.filter(address=bare_ip))
                    if netbox_ips:
                        netbox_ip = netbox_ips[0]
                        self._enrich_ip(netbox_ip, interface)
                        return netbox_ip
                raise
            return netbox_ip

        netbox_ip = list(netbox_ips)[0]
        # If IP exists in anycast
        if netbox_ip.role and netbox_ip.role.label == "Anycast":
            logging.debug("IP {} is Anycast..".format(ip))
            unassigned_anycast_ip = [x for x in netbox_ips if x.interface is None]
            assigned_anycast_ip = [
                x for x in netbox_ips if x.interface and x.interface.id == interface.id
            ]
            # use the first available anycast ip
            if len(unassigned_anycast_ip):
                logging.info("Assigning existing Anycast IP {} to interface".format(ip))
                netbox_ip = unassigned_anycast_ip[0]
                netbox_ip.interface = interface
                netbox_ip.save()
            # or if everything is assigned to other servers
            elif not len(assigned_anycast_ip):
                logging.info("Creating Anycast IP {} and assigning it to interface".format(ip))
                query_params = {
                    "address": ip,
                    "status": "active",
                    "role": self.ipam_choices["ip-address:role"]["Anycast"],
                    "tenant": self.tenant.id if self.tenant else None,
                    "assigned_object_type": self.assigned_object_type,
                    "assigned_object_id": interface.id,
                    "dns_name": self._ip_dns_name(),
                }
                netbox_ip = nb.ipam.ip_addresses.create(**query_params)
            return netbox_ip
        else:
            assigned_object = getattr(netbox_ip, "assigned_object", None)
            if not assigned_object:
                logging.info(
                    "Assigning existing IP {ip} to {interface}".format(ip=ip, interface=interface)
                )
            elif assigned_object.id != interface.id:
                old_interface = getattr(netbox_ip, "assigned_object", "n/a")
                logging.info(
                    "Detected interface change for ip {ip}: old interface is "
                    "{old_interface} (id: {old_id}), new interface is {new_interface} "
                    " (id: {new_id})".format(
                        old_interface=old_interface,
                        new_interface=interface,
                        old_id=netbox_ip.id,
                        new_id=interface.id,
                        ip=netbox_ip.address,
                    )
                )
            else:
                # IP already on correct interface — still update dns_name/tenant
                self._enrich_existing_ip(netbox_ip)
                return netbox_ip

            self._enrich_ip(netbox_ip, interface)
            return netbox_ip

    def _ip_dns_name(self):
        """Return the hostname to use as dns_name on IP addresses."""
        try:
            return self.server.get_hostname()
        except Exception:
            return ""

    def _enrich_existing_ip(self, netbox_ip):
        """Update dns_name and tenant on an IP that is already correctly assigned.

        Only saves if something actually changed to avoid unnecessary API calls.
        """
        dirty = False
        dns = self._ip_dns_name()
        if dns and getattr(netbox_ip, "dns_name", None) != dns:
            netbox_ip.dns_name = dns
            dirty = True
        if self.tenant and getattr(netbox_ip, "tenant", None) != self.tenant:
            netbox_ip.tenant = self.tenant.id
            dirty = True
        if dirty:
            logging.info("Enriching IP %s: dns_name=%s tenant=%s",
                         netbox_ip.address, dns, self.tenant)
            netbox_ip.save()

    def _enrich_ip(self, netbox_ip, interface):
        """Set dns_name, tenant, and interface assignment on an existing IP."""
        netbox_ip.assigned_object_type = self.assigned_object_type
        netbox_ip.assigned_object_id = interface.id
        dns = self._ip_dns_name()
        if dns and getattr(netbox_ip, "dns_name", None) != dns:
            netbox_ip.dns_name = dns
        if self.tenant and getattr(netbox_ip, "tenant", None) != self.tenant:
            netbox_ip.tenant = self.tenant.id
        netbox_ip.save()

    def _nic_identifier(self, nic):
        if isinstance(nic, dict):
            if config.network.nic_id == "mac":
                if not nic["mac"]:
                    logging.warning(
                        "%s: MAC not available while trying to use it as the NIC identifier",
                        nic["name"],
                    )
                return nic["mac"]
            return nic["name"]
        else:
            if config.network.nic_id == "mac":
                if not nic.mac_address:
                    logging.warning(
                        "%s: MAC not available while trying to use it as the NIC identifier",
                        nic.name,
                    )
                return nic.mac_address
            return nic.name

    def create_or_update_netbox_network_cards(self):
        if config.update_all is None or config.update_network is None:
            return None
        _clear_nic_module_cache()
        logging.debug("Creating/Updating NIC...")

        # delete unknown interface — but respect managed_by ownership.
        # Interfaces created by other workers (bmc-scan, proxmox-sync)
        # may not be visible to the OS and must not be deleted.
        nb_nics = list(self.get_netbox_network_cards())
        local_nics = [self._nic_identifier(x) for x in self.nics]
        for nic in list(nb_nics):
            if self._nic_identifier(nic) not in local_nics:
                managed_by = (nic.custom_fields or {}).get("managed_by", "")
                if managed_by and managed_by != "netbox-agent":
                    logging.debug(
                        "Skipping deletion of '%s' (managed_by=%s)",
                        nic.name, managed_by,
                    )
                    continue
                logging.info(
                    "Deleting netbox interface {name} because not present locally".format(
                        name=nic.name
                    )
                )
                nb_nics.remove(nic)
                nic.delete()

        # delete IP on netbox that are not known on this server
        if len(nb_nics):

            def batched(it, n):
                while batch := tuple(islice(it, n)):
                    yield batch

            netbox_ips = []
            for ids in batched((x.id for x in nb_nics), 25):
                netbox_ips += list(nb.ipam.ip_addresses.filter(**{self.intf_type: ids}))

            all_local_ips = list(
                chain.from_iterable([x["ip"] for x in self.nics if x["ip"] is not None])
            )
            for netbox_ip in netbox_ips:
                if netbox_ip.address not in all_local_ips:
                    # If this IP is the device's primary_ip4, clear it first —
                    # NetBox refuses to unassign an IP that is still designated
                    # as primary (returns 400 Bad Request).
                    device_primary = getattr(self.device, "primary_ip4", None)
                    if device_primary and device_primary.id == netbox_ip.id:
                        logging.info(
                            "Clearing primary_ip4 %s on device %s before unassigning",
                            netbox_ip.address,
                            getattr(self.device, "name", "?"),
                        )
                        # Re-fetch to avoid stale state
                        fresh_device = nb.dcim.devices.get(self.device.id)
                        fresh_device.primary_ip4 = None
                        try:
                            fresh_device.save()
                        except Exception as e:
                            # NetBox may validate other IP fields (e.g., oob_ip)
                            # that reference IPs not assigned to the device.
                            # Clear those too and retry.
                            err_str = str(e)
                            if "oob_ip" in err_str:
                                logging.warning(
                                    "oob_ip validation failed during primary_ip4 clear — "
                                    "also clearing oob_ip: %s", e,
                                )
                                fresh_device.oob_ip = None
                                fresh_device.save()
                            else:
                                raise
                        # Update local reference so downstream code sees the change
                        self.device = nb.dcim.devices.get(self.device.id)

                    # Clear oob_ip if it points to this IP (NetBox blocks
                    # unassigning an IP that is designated as oob_ip).
                    device_oob = getattr(self.device, "oob_ip", None)
                    if device_oob and device_oob.id == netbox_ip.id:
                        logging.info(
                            "Clearing oob_ip %s before unassigning from %s",
                            netbox_ip.address,
                            getattr(self.device, "name", "?"),
                        )
                        fresh_device = nb.dcim.devices.get(self.device.id)
                        fresh_device.oob_ip = None
                        fresh_device.save()
                        self.device = nb.dcim.devices.get(self.device.id)

                    logging.info(
                        "Unassigning IP {ip} from {interface}".format(
                            ip=netbox_ip.address, interface=netbox_ip.assigned_object
                        )
                    )
                    netbox_ip.assigned_object_type = None
                    netbox_ip.assigned_object_id = None
                    netbox_ip.save()

        # update each nic
        for nic in self.nics:
            interface = self.get_netbox_network_card(nic)

            # IPMI interface should be management-only
            is_ipmi = nic.get("ipmi", False)

            if not interface:
                logging.info(
                    "Interface {nic} not found, creating..".format(nic=self._nic_identifier(nic))
                )
                interface = self.create_netbox_nic(nic, mgmt=is_ipmi)

            nic_update = 0

            # Ensure mgmt_only is correct (fix existing interfaces)
            if is_ipmi and not interface.mgmt_only:
                logging.info("Setting mgmt_only=True on IPMI interface")
                interface.mgmt_only = True
                nic_update += 1

            ret, interface = self.reset_vlan_on_interface(nic, interface)
            nic_update += ret

            if nic["name"] != interface.name:
                logging.info(
                    "Updating interface {interface} name to: {name}".format(
                        interface=interface, name=nic["name"]
                    )
                )
                interface.name = nic["name"]
                nic_update += 1

            if version.parse(nb.version) >= version.parse("4.2"):
                # Sync MAC objects and set primary_mac_address (by ID)
                if nic["mac"]:
                    mac_objs = self.update_interface_macs(interface, self._all_macs(nic))
                    # Find the MAC object matching nic["mac"] and set as primary
                    primary_mac_id = None
                    for mac_obj in (mac_objs or []):
                        if mac_obj.mac_address and mac_obj.mac_address.upper() == nic["mac"].upper():
                            primary_mac_id = mac_obj.id
                            break
                    current_primary = getattr(interface, "primary_mac_address", None)
                    current_primary_id = current_primary.id if current_primary else None
                    if primary_mac_id and primary_mac_id != current_primary_id:
                        logging.info(
                            "Setting primary MAC on {interface} to {mac}".format(
                                interface=interface, mac=nic["mac"]
                            )
                        )
                        interface.primary_mac_address = primary_mac_id
                        nic_update += 1
            else:
                if nic["mac"] and nic["mac"] != interface.mac_address:
                    logging.info(
                        "Updating interface {interface} mac to: {mac}".format(
                            interface=interface, mac=nic["mac"]
                        )
                    )
                    interface.mac_address = nic["mac"]
                    nic_update += 1

            if hasattr(interface, "mtu"):
                if nic["mtu"] != interface.mtu:
                    logging.info(
                        "Interface mtu is wrong, updating to: {mtu}".format(mtu=nic["mtu"])
                    )
                    interface.mtu = nic["mtu"]
                    nic_update += 1

            if not isinstance(self, VirtualNetwork) and nic.get("ethtool"):
                if (
                    nic["ethtool"]["duplex"] != "-"
                    and interface.duplex != nic["ethtool"]["duplex"].lower()
                ):
                    interface.duplex = nic["ethtool"]["duplex"].lower()
                    nic_update += 1

                if nic["ethtool"]["speed"] != "-":
                    speed = int(
                        nic["ethtool"]["speed"].replace("Mb/s", "000").replace("Gb/s", "000000")
                    )
                    if speed != interface.speed:
                        interface.speed = speed
                        nic_update += 1

            if hasattr(interface, "type"):
                _type = self.get_netbox_type_for_nic(nic)
                if not interface.type or _type != interface.type.value:
                    logging.info("Interface type is wrong, resetting")
                    interface.type = _type
                    nic_update += 1

            # Update transceiver description and create Module if ethtool reports module info
            if not isinstance(self, VirtualNetwork) and nic.get("ethtool"):
                transceiver_desc = _build_transceiver_description(nic["ethtool"])
                if transceiver_desc and (interface.description or "") != transceiver_desc:
                    interface.description = transceiver_desc
                    nic_update += 1

                # Create transceiver Module linked to this interface
                _sync_transceiver_module(self.device.id, interface, nic["ethtool"])

            if hasattr(interface, "lag") and interface.lag is not None:
                local_lag_int = next(
                    item for item in self.nics if item["name"] == interface.lag.name
                )
                if nic["name"] not in local_lag_int["bonding_slaves"]:
                    logging.info("Interface has no LAG, resetting")
                    nic_update += 1
                    interface.lag = None

            # cable the interface — never on a bond/LAG iface itself
            # (NetBox 4.x rejects cables on type=lag, INF-320).
            if (
                config.network.lldp
                and isinstance(self, ServerNetwork)
                and not nic.get("bonding")
            ):
                switch_ip = self.lldp.get_switch_ip(interface.name)
                switch_interface = self.lldp.get_switch_port(interface.name)
                if switch_ip and switch_interface:
                    ret, interface = self.create_or_update_cable(
                        switch_ip, switch_interface, interface
                    )
                    nic_update += ret

            if nic["ip"]:
                # sync local IPs
                for ip in nic["ip"]:
                    self.create_or_update_netbox_ip_on_interface(ip, interface)
            if nic_update > 0:
                interface.save()

        self._set_bonding_interfaces()
        logging.debug("Finished updating NIC!")


class ServerNetwork(Network):
    def __init__(self, server, *args, **kwargs):
        super(ServerNetwork, self).__init__(server, args, kwargs)

        if config.network.ipmi:
            self.ipmi = self.get_ipmi()
        if self.ipmi:
            self.nics.append(self.ipmi)

        self.server = server
        self.device = self.server.get_netbox_server()
        self.nb_net = nb.dcim
        self.custom_arg = {"device": getattr(self.device, "id", None)}
        self.custom_arg_id = {"device_id": getattr(self.device, "id", None)}
        self.intf_type = "interface_id"
        self.assigned_object_type = "dcim.interface"

    def get_network_type(self):
        return "server"

    def get_ipmi(self):
        ipmi = IPMI().parse()
        return ipmi

    def connect_interface_to_switch(self, switch_ip, switch_interface, nb_server_interface):
        logging.info(
            "Interface {} is not connected to switch, trying to connect..".format(
                nb_server_interface.name
            )
        )
        nb_mgmt_ip = nb.ipam.ip_addresses.get(
            address=switch_ip,
        )
        if not nb_mgmt_ip:
            logging.error("Switch IP {} cannot be found in Netbox".format(switch_ip))
            return nb_server_interface

        try:
            nb_switch = nb_mgmt_ip.assigned_object.device
            logging.info(
                "Found a switch in Netbox based on LLDP infos: {} (id: {})".format(
                    switch_ip, nb_switch.id
                )
            )
        except KeyError:
            logging.error(
                "Switch IP {} is found but not associated to a Netbox Switch Device".format(
                    switch_ip
                )
            )
            return nb_server_interface

        switch_interface = self.lldp.get_switch_port(nb_server_interface.name)
        nb_switch_interface = nb.dcim.interfaces.get(
            device_id=nb_switch.id,
            name=switch_interface,
        )
        if nb_switch_interface is None:
            logging.error("Switch interface {} cannot be found".format(switch_interface))
            return nb_server_interface

        logging.info(
            "Found interface {} on switch {}".format(
                switch_interface,
                switch_ip,
            )
        )
        cable = nb.dcim.cables.create(
            a_terminations=[
                {"object_type": "dcim.interface", "object_id": nb_server_interface.id},
            ],
            b_terminations=[
                {"object_type": "dcim.interface", "object_id": nb_switch_interface.id},
            ],
        )
        nb_server_interface.cable = cable
        logging.info(
            "Connected interface {interface} with {switch_interface} of {switch_ip}".format(
                interface=nb_server_interface.name,
                switch_interface=switch_interface,
                switch_ip=switch_ip,
            )
        )
        return nb_server_interface

    def create_or_update_cable(self, switch_ip, switch_interface, nb_server_interface):
        update = False
        if nb_server_interface.cable is None:
            update = True
            nb_server_interface = self.connect_interface_to_switch(
                switch_ip, switch_interface, nb_server_interface
            )
        else:
            nb_sw_int = nb_server_interface.cable.b_terminations[0]
            nb_sw = nb_sw_int.device
            nb_mgmt_int = nb.dcim.interfaces.get(device_id=nb_sw.id, mgmt_only=True)
            nb_mgmt_ip = nb.ipam.ip_addresses.get(interface_id=nb_mgmt_int.id)
            if nb_mgmt_ip is None:
                logging.error(
                    "Switch {switch_ip} does not have IP on its management interface".format(
                        switch_ip=switch_ip,
                    )
                )
                return update, nb_server_interface

            # Netbox IP is always IP/Netmask
            nb_mgmt_ip = nb_mgmt_ip.address.split("/")[0]
            if nb_mgmt_ip != switch_ip or nb_sw_int.name != switch_interface:
                logging.info("Netbox cable is not connected to correct ports, fixing..")
                logging.info(
                    "Deleting cable {cable_id} from {interface} to {switch_interface} of "
                    "{switch_ip}".format(
                        cable_id=nb_server_interface.cable.id,
                        interface=nb_server_interface.name,
                        switch_interface=nb_sw_int.name,
                        switch_ip=nb_mgmt_ip,
                    )
                )
                cable = nb.dcim.cables.get(nb_server_interface.cable.id)
                cable.delete()
                update = True
                nb_server_interface = self.connect_interface_to_switch(
                    switch_ip, switch_interface, nb_server_interface
                )
        return update, nb_server_interface


class VirtualNetwork(Network):
    def __init__(self, server, *args, **kwargs):
        super(VirtualNetwork, self).__init__(server, args, kwargs)
        self.server = server
        self.device = self.server.get_netbox_vm()
        self.nb_net = nb.virtualization
        self.custom_arg = {"virtual_machine": getattr(self.device, "id", None)}
        self.custom_arg_id = {"virtual_machine_id": getattr(self.device, "id", None)}
        self.intf_type = "vminterface_id"
        self.assigned_object_type = "virtualization.vminterface"

        dcim_c = nb.virtualization.interfaces.choices()
        for _choice_type in dcim_c:
            key = "interface:{}".format(_choice_type)
            self.dcim_choices[key] = {}
            for choice in dcim_c[_choice_type]:
                self.dcim_choices[key][choice["display_name"]] = choice["value"]

    def get_network_type(self):
        return "virtual"

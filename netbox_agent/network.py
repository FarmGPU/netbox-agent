import ipaddress
import json
import logging
import os
import re
import subprocess
from itertools import chain, islice
from pathlib import Path

import netifaces
from netaddr import IPAddress
from packaging import version
from pynetbox.core.query import RequestError

from netbox_agent.config import config
from netbox_agent.config import netbox_instance as nb
from netbox_agent.ethtool import Ethtool
from netbox_agent.ipmi import IPMI
from netbox_agent.lldp import LLDP
from netbox_agent.misc import is_tool

VIRTUAL_NET_FOLDER = Path("/sys/devices/virtual/net")

# Custom field carrying dns_name provenance. See _may_write_dns_name.
DNS_NAME_OWNER_CF = "dns_name_managed_by"
DNS_NAME_OWNER = "netbox-agent"

# Per-run caches. The agent is a short-lived process, so "once per run" is the
# right lifetime for both — a fleet of hosts each doing one prefix fetch is far
# cheaper than one `contains=` query per address.
_prefix_cache = None
_dns_cf_present = None


def _load_vrf_prefixes():
    """Every VRF-owned, non-container prefix as (network, vrf_id, vrf_name).

    Fetched once per run. Container prefixes are skipped because they exist to
    group other prefixes, not to own addresses — matching one would attribute an
    IP to a VRF whose child prefix may say otherwise.
    """
    global _prefix_cache
    if _prefix_cache is not None:
        return _prefix_cache

    cache = []
    try:
        for prefix in nb.ipam.prefixes.all():
            status = getattr(prefix.status, "value", None) or str(prefix.status or "")
            if status == "container" or not prefix.vrf:
                continue
            try:
                cache.append(
                    (ipaddress.ip_network(str(prefix.prefix)), prefix.vrf.id, prefix.vrf.name)
                )
            except ValueError:
                logging.debug("Skipping unparseable prefix %r", getattr(prefix, "prefix", None))
    except Exception:
        # A failed fetch must not abort the sync: an unknown VRF scope degrades
        # to the pre-DEV-91 behaviour (everything lands in the global table),
        # which is wrong but recoverable on the next run.
        logging.warning("Could not load prefixes for VRF resolution", exc_info=True)
        cache = []

    _prefix_cache = cache
    return _prefix_cache


def vrf_for_address(address):
    """VRF id owning *address*, or None when it belongs in the global table.

    Longest-match wins, mirroring how a router would resolve it. A tie between
    two VRFs at the same prefix length is unresolvable here, so it returns None
    and leaves the address global rather than guessing — an operator can see a
    global IP and fix it, but a silently wrong VRF looks correct.
    """
    try:
        addr = ipaddress.ip_address(str(address).split("/")[0])
    except ValueError:
        return None

    candidates = [(net, vrf_id, name) for net, vrf_id, name in _load_vrf_prefixes() if addr in net]
    if not candidates:
        return None

    longest = max(net.prefixlen for net, _, _ in candidates)
    winners = [c for c in candidates if c[0].prefixlen == longest]
    if len(winners) > 1:
        logging.warning(
            "Ambiguous VRF for %s: %d prefixes tie at /%d (%s); leaving it in the global table",
            address,
            len(winners),
            longest,
            ", ".join(sorted(name for _, _, name in winners)),
        )
        return None

    return winners[0][1]


def _dns_provenance_available():
    """Whether NetBox has the dns_name provenance custom field.

    Checked once per run. When absent, the agent still protects hand-set names
    via the hostname comparison in _may_write_dns_name — it simply cannot track
    a legitimate hostname change on a name it owns.
    """
    global _dns_cf_present
    if _dns_cf_present is None:
        try:
            _dns_cf_present = bool(list(nb.extras.custom_fields.filter(name=DNS_NAME_OWNER_CF)))
        except Exception:
            _dns_cf_present = False
        if not _dns_cf_present:
            logging.warning(
                "Custom field '%s' is not defined in NetBox; dns_name ownership will "
                "fall back to comparing the stored name against the current hostname",
                DNS_NAME_OWNER_CF,
            )
    return _dns_cf_present


def _get_ovs_bonds():
    """Return {bond_name: [slave_names]} for Open vSwitch-managed bonds.

    OVS bonds are managed by ovs-vswitchd, not the Linux bonding driver, so
    they don't expose `/sys/class/net/<bond>/bonding/`. Without this lookup,
    the agent treats them as generic virtual interfaces — which on Proxmox
    hosts (the main consumers of OVS in our fleet) silently corrupts the LAG
    model: bond gets typed `virtual`, slaves never get `lag` pointers, and
    NetBox can end up rejecting writes that were valid in the kernel-bond
    case (INF-322).

    Returns an empty dict when ovs-appctl isn't available or returns an
    error — callers should fall back to kernel-bond detection only.
    """
    if not is_tool("ovs-appctl"):
        return {}
    try:
        out = subprocess.check_output(
            ["ovs-appctl", "bond/list"], encoding="utf-8", timeout=5,
        )
    except (subprocess.SubprocessError, OSError) as e:
        logging.debug("ovs-appctl bond/list failed: %s", e)
        return {}

    # Output format (tab-separated):
    #   bond    type            recircID    members
    #   bond0   balance-tcp     1           enp129s0f0np0, enp129s0f1np1
    bonds = {}
    for line in out.splitlines()[1:]:  # skip header
        parts = line.split("\t")
        if len(parts) < 4:
            continue
        bond_name = parts[0].strip()
        members = [m.strip() for m in parts[3].split(",") if m.strip()]
        if bond_name and members:
            bonds[bond_name] = members
    return bonds


def _default_route_iface():
    """Return the iface name of the IPv4 default route, or None.

    The default-route iface is the management interface by definition —
    it's the path used to reach this host from outside. scan() uses this
    so the ignore_interfaces regex can never filter out the iface that
    owns primary_ip4. Without the guarantee, hosts that put their mgmt
    IP on a Linux bridge (standalone Proxmox: vmbr0) or on any iface
    that happens to match an operator's ignore pattern would have
    primary_ip4 silently fail to resolve.
    """
    try:
        out = subprocess.check_output(
            ["ip", "-j", "route", "show", "default"],
            encoding="utf-8", timeout=5,
        )
    except (subprocess.SubprocessError, OSError) as e:
        logging.debug("ip route show default failed: %s", e)
        return None
    try:
        routes = json.loads(out)
    except (ValueError, TypeError):
        return None
    if routes and isinstance(routes, list):
        return routes[0].get("dev")
    return None


def _is_bridge(name):
    """True iff /sys/class/net/<name> is a Linux bridge."""
    return os.path.isdir(f"/sys/class/net/{name}/bridge")


def _get_bridge_members(name):
    """Return the list of port-member iface names for a Linux bridge.

    Linux bridges expose port-members under /sys/class/net/<br>/brif/<port>.
    Empty list for non-bridges or unreadable bridges. Members may include
    physical NICs (eno1), bonds (bond0), or VM/container endpoints
    (tap*, veth*) — callers filter by what survived ignore_interfaces.
    """
    brif = f"/sys/class/net/{name}/brif"
    if not os.path.isdir(brif):
        return []
    try:
        return sorted(os.listdir(brif))
    except OSError:
        return []


def _is_sriov_vf_netdev(name):
    """True iff the netdev is an SR-IOV virtual function.

    sysfs exposes /sys/class/net/<name>/device/physfn as a symlink iff the
    netdev belongs to a VF; the symlink points back to the parent PF.
    Mirrors modules._is_sriov_vf (INF-321) for the netdev side — VFs share
    the PF's wire, so tracking them as separate ifaces is double-counting,
    and lldpd reporting the same neighbor for VF and PF makes the per-iface
    cable creation path collide on duplicate terminations.
    """
    return os.path.islink(f"/sys/class/net/{name}/device/physfn")


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
            # No custom_fields: "owner" is defined on no instance and NetBox 4.6
            # rejects the whole write for an unknown key. See
            # server.py::_netbox_create_server.
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
        # The default-route iface is the host's management interface by
        # definition. Always enroll it, even if it matches the ignore
        # regex (e.g., standalone Proxmox where mgmt lives on vmbr0).
        # Without this carve-out, primary_ip4 can never resolve. Logged
        # at INFO when the override fires so it's visible in journalctl.
        mgmt_iface = _default_route_iface() or ""
        # OVS bonds aren't visible through /sys/class/net/<bond>/bonding;
        # query ovs-appctl once up-front so the per-iface loop below can
        # treat OVS and kernel bonds uniformly. Empty dict when OVS isn't
        # installed (the common case on bare-metal Linux).
        ovs_bonds = _get_ovs_bonds()
        for interface in os.listdir("/sys/class/net/"):
            # ignore if it's not a link (ie: bonding_masters etc)
            if not os.path.islink("/sys/class/net/{}".format(interface)):
                continue

            # Skip SR-IOV VFs — they share the PF's wire and shouldn't be
            # tracked as separate ifaces. lldpd reports the same neighbor
            # for the VF as for its parent PF, so without this skip the
            # per-iface cable creation path tries to write a second cable
            # to the same switch port and fails with HTTP 400 "Duplicate
            # termination". Existing NetBox VF records get pruned by the
            # "not present locally" deletion path in
            # create_or_update_netbox_network_cards. Mirror of
            # modules._is_sriov_vf (INF-321).
            if _is_sriov_vf_netdev(interface):
                logging.debug("Skipping SR-IOV VF: %s", interface)
                continue

            if ignore_re and ignore_re.match(interface):
                if interface == mgmt_iface:
                    logging.info(
                        "Default-route iface %s matched ignore_interfaces; "
                        "enrolling anyway so primary_ip4 can resolve.",
                        interface,
                    )
                else:
                    logging.debug(
                        "Ignore interface {interface}".format(interface=interface)
                    )
                    continue

            ip_addr = netifaces.ifaddresses(interface).get(netifaces.AF_INET, [])
            ip6_addr = netifaces.ifaddresses(interface).get(netifaces.AF_INET6, [])

            # Linux IPv4 alias labels (e.g., 'enp65s0f1:e') attach an IP to
            # a parent iface under a separate label name. netifaces exposes
            # each label as its own pseudo-interface, but the IP really
            # belongs to the parent — `/sys/class/net` only lists the bare
            # iface, so without this loop the labeled alias's IP was
            # silently dropped (seen on VAST appliances where the mgmt IP
            # is configured this way). Pick up both families for symmetry.
            alias_prefix = "{}:".format(interface)
            for alias in netifaces.interfaces():
                if not alias.startswith(alias_prefix):
                    continue
                alias_ifaddrs = netifaces.ifaddresses(alias)
                ip_addr.extend(alias_ifaddrs.get(netifaces.AF_INET, []))
                ip6_addr.extend(alias_ifaddrs.get(netifaces.AF_INET6, []))

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
            elif interface in ovs_bonds:
                # Open vSwitch-managed bond (Proxmox/OVS hosts). See
                # _get_ovs_bonds() and INF-322 for the why.
                bonding = True
                bonding_slaves = ovs_bonds[interface]

            # Linux bridge detection mirrors the bonding lookup above.
            # The bridge holds the IP; physical port-members carry the
            # cable. _set_bridge_interfaces() points each member's
            # iface.bridge at the bridge iface afterwards (parallel to
            # _set_bonding_interfaces() for LAG slaves).
            bridge = _is_bridge(interface)
            bridge_members = _get_bridge_members(interface) if bridge else []

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
                "bridge": bridge,
                "bridge_members": bridge_members,
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

    def _set_bridge_interfaces(self):
        """Point each bridge port-member's iface.bridge at the bridge iface.

        Mirrors _set_bonding_interfaces() for LAG slaves. Once populated,
        NetBox can navigate bridge → physical port-member → cable →
        switch port — same shape as VLAN subiface → parent → cable.
        Members that were filtered out (tap*, veth*, fwbr* under the
        Proxmox ignore pattern) aren't in self.nics, so they're skipped
        naturally without an HTTP roundtrip per VM.
        """
        bridge_nics = [x for x in self.nics if x.get("bridge")]
        for nic in bridge_nics:
            bridge_int = self.get_netbox_network_card(nic)
            if not bridge_int:
                continue
            members = nic.get("bridge_members") or []
            for member_int in (
                self.get_netbox_network_card(member_nic)
                for member_nic in self.nics
                if member_nic["name"] in members
            ):
                if member_int is None:
                    continue
                current = getattr(member_int, "bridge", None)
                if current is None or current.id != bridge_int.id:
                    logging.debug(
                        "Setting iface %s bridge=%s",
                        member_int.name, bridge_int.name,
                    )
                    member_int.bridge = bridge_int
                    member_int.save()

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

        # Bridges must be checked before the generic `virtual` fallback;
        # they live under /sys/devices/virtual/net/ so they'd otherwise
        # be typed Virtual. Bridge is a dedicated NetBox interface type
        # (since 3.0) — parallels typing bonds as LAG rather than Virtual.
        if nic.get("bridge"):
            return self.dcim_choices["interface:type"]["Bridge"]

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

        # cable the interface — but never on a bond/LAG or bridge iface
        # itself. NetBox rejects cables on type=lag; bridges are software
        # constructs with no wire. Cables belong on the physical
        # members, which iterate as separate nics in the outer loop
        # (INF-320 for LAG slaves; same reasoning extends to bridge
        # port-members).
        if (
            config.network.lldp
            and isinstance(self, ServerNetwork)
            and not nic.get("bonding")
            and not nic.get("bridge")
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
        # VRF the address belongs to, from the containing prefix (DEV-91).
        # Resolved before the lookup because it scopes which record we want.
        vrf_id = vrf_for_address(ip)

        netbox_ips = nb.ipam.ip_addresses.filter(
            address=ip,
        )
        # Also search by bare IP (without prefix) — BMC API may have stored
        # the same IP with a different prefix length (e.g., /32 vs /20)
        if not netbox_ips:
            bare_ip = ip.split("/")[0]
            netbox_ips = nb.ipam.ip_addresses.filter(address=bare_ip)

        netbox_ips = list(netbox_ips)

        if not netbox_ips:
            logging.info("Create new IP {ip} on {interface}".format(ip=ip, interface=interface))
            query_params = {
                "address": ip,
                "status": "active",
                "assigned_object_type": self.assigned_object_type,
                "assigned_object_id": interface.id,
            }
            dns = self._ip_dns_name()
            if dns:
                query_params["dns_name"] = dns
                if _dns_provenance_available():
                    query_params["custom_fields"] = {DNS_NAME_OWNER_CF: DNS_NAME_OWNER}
            if vrf_id:
                query_params["vrf"] = vrf_id
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

        netbox_ip = self._select_ip_for_vrf(netbox_ips, vrf_id, ip)
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
                }
                dns = self._ip_dns_name()
                if dns:
                    query_params["dns_name"] = dns
                    if _dns_provenance_available():
                        query_params["custom_fields"] = {DNS_NAME_OWNER_CF: DNS_NAME_OWNER}
                if vrf_id:
                    query_params["vrf"] = vrf_id
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

    def _may_write_dns_name(self, netbox_ip):
        """Whether the agent owns dns_name on this record (DEV-94).

        The agent writes dns_name when it is unset, or when the agent wrote it
        last. A name set by anyone else is left alone: at sites where tenants
        can rename their own machines, the OS hostname is not an identity we
        control, and propagating it destroys the operator-set name on every
        run.

        Ownership lives in the `dns_name_managed_by` custom field, stamped
        whenever the agent writes the name. Records predating the field carry
        no value, so a stored name identical to what we would write now is
        treated as ours — that adopts existing agent-written names with no
        backfill, while leaving hand-set names protected.
        """
        if netbox_ip is None:
            return True

        current = getattr(netbox_ip, "dns_name", "") or ""
        if not current:
            return True

        owner = (getattr(netbox_ip, "custom_fields", None) or {}).get(DNS_NAME_OWNER_CF) or ""
        if owner:
            return owner == DNS_NAME_OWNER

        return current == self._ip_dns_name()

    def _stamp_dns_name(self, netbox_ip, dns):
        """Set dns_name and record that the agent owns it. Returns True if changed."""
        if getattr(netbox_ip, "dns_name", None) == dns:
            return False

        logging.info(
            "Setting dns_name=%s on IP %s (was %r)",
            dns, netbox_ip.address, getattr(netbox_ip, "dns_name", None),
        )
        netbox_ip.dns_name = dns
        if _dns_provenance_available():
            custom_fields = dict(getattr(netbox_ip, "custom_fields", None) or {})
            custom_fields[DNS_NAME_OWNER_CF] = DNS_NAME_OWNER
            netbox_ip.custom_fields = custom_fields
        return True

    def _select_ip_for_vrf(self, netbox_ips, vrf_id, ip):
        """Choose which existing record to use, honouring VRF (DEV-91).

        Never creates a duplicate. When the only record sits in the global
        table but the address belongs to a VRF-owned prefix, that record is
        MOVED into the VRF rather than a second one being created — the agent
        wrote those records, and a move is reversible where a duplicate is not.

        When records exist in both places the VRF one wins and the global one
        is left untouched: deciding which of two populated records is correct
        needs a human, not a heuristic.
        """
        if vrf_id is None or not netbox_ips:
            return netbox_ips[0] if netbox_ips else None

        in_vrf = [x for x in netbox_ips if x.vrf and x.vrf.id == vrf_id]
        in_global = [x for x in netbox_ips if not x.vrf]

        if in_vrf:
            if in_global:
                logging.warning(
                    "IP %s exists both in VRF id=%s (record %s) and in the global table "
                    "(record %s); using the VRF record and leaving the global duplicate "
                    "for manual merge",
                    ip, vrf_id, in_vrf[0].id, in_global[0].id,
                )
            return in_vrf[0]

        if in_global:
            record = in_global[0]
            logging.info(
                "Moving IP %s (record %s) from the global table into VRF id=%s",
                ip, record.id, vrf_id,
            )
            record.vrf = vrf_id
            try:
                record.save()
            except Exception:
                logging.warning(
                    "Could not move IP %s into VRF id=%s; leaving it global",
                    ip, vrf_id, exc_info=True,
                )
            return record

        return netbox_ips[0]

    def _enrich_existing_ip(self, netbox_ip):
        """Update dns_name and tenant on an IP that is already correctly assigned.

        Only saves if something actually changed to avoid unnecessary API calls.
        """
        dirty = False
        dns = self._ip_dns_name()
        if dns and self._may_write_dns_name(netbox_ip):
            dirty = self._stamp_dns_name(netbox_ip, dns) or dirty
        if self.tenant and getattr(netbox_ip, "tenant", None) != self.tenant:
            netbox_ip.tenant = self.tenant.id
            dirty = True
        if dirty:
            logging.info("Enriching IP %s: dns_name=%s tenant=%s",
                         netbox_ip.address, netbox_ip.dns_name, self.tenant)
            netbox_ip.save()

    def _enrich_ip(self, netbox_ip, interface):
        """Set dns_name, tenant, and interface assignment on an existing IP.

        The host's `ip addr show` is the source of truth for which iface
        owns an IP — if a NIC carries it on the wire, NetBox should
        reflect that. NetBox 4.x rejects the iface reassignment patch
        when the IP is currently designated as the parent object's
        primary_ip4 / oob_ip; we clear that reference first so the
        reassignment succeeds.

        Two parent-object shapes the IP may currently belong to:

        - dcim.interface  -> parent is a Device (has primary_ip4 + oob_ip)
        - virtualization.vminterface -> parent is a VirtualMachine
                                        (has primary_ip4 only)

        Cross-namespace cases (VM iface holds an IP that a bare-metal
        host now claims) are usually stale proxmox-sync records that
        weren't pruned after VM decommission — same treatment, clear
        the VM's primary_ip4 reference and let proxmox-sync's next
        cycle reconcile the orphan.
        """
        old_obj_type = netbox_ip.assigned_object_type
        old_obj_id = netbox_ip.assigned_object_id
        new_obj_type = self.assigned_object_type
        new_obj_id = interface.id

        if old_obj_id and (old_obj_id != new_obj_id or old_obj_type != new_obj_type):
            try:
                current_owner = None
                current_iface_name = None
                if old_obj_type == "dcim.interface":
                    ci = nb.dcim.interfaces.get(old_obj_id)
                    if ci and ci.device:
                        current_iface_name = ci.name
                        current_owner = nb.dcim.devices.get(ci.device.id)
                elif old_obj_type == "virtualization.vminterface":
                    vi = nb.virtualization.interfaces.get(old_obj_id)
                    if vi and vi.virtual_machine:
                        current_iface_name = vi.name
                        current_owner = nb.virtualization.virtual_machines.get(
                            vi.virtual_machine.id
                        )

                if current_owner:
                    updates = {}
                    if (
                        current_owner.primary_ip4
                        and current_owner.primary_ip4.id == netbox_ip.id
                    ):
                        updates["primary_ip4"] = None
                    # oob_ip exists on Device, not on VirtualMachine — guard
                    if (
                        getattr(current_owner, "oob_ip", None)
                        and current_owner.oob_ip.id == netbox_ip.id
                    ):
                        updates["oob_ip"] = None
                    if updates:
                        logging.info(
                            "Clearing %s on %s (%s/%s) before reassigning IP "
                            "%s to %s",
                            list(updates.keys()),
                            current_owner.name,
                            old_obj_type,
                            current_iface_name,
                            netbox_ip.address,
                            interface.name,
                        )
                        current_owner.update(updates)
            except Exception:
                logging.debug(
                    "Pre-reassign primary_ip4/oob_ip clear failed (proceeding anyway)",
                    exc_info=True,
                )

        netbox_ip.assigned_object_type = self.assigned_object_type
        netbox_ip.assigned_object_id = interface.id
        dns = self._ip_dns_name()
        if dns and self._may_write_dns_name(netbox_ip):
            self._stamp_dns_name(netbox_ip, dns)
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

            # cable the interface — never on a bond/LAG or bridge iface
            # itself (NetBox 4.x rejects cables on type=lag, INF-320;
            # bridges are software constructs with no wire).
            if (
                config.network.lldp
                and isinstance(self, ServerNetwork)
                and not nic.get("bonding")
                and not nic.get("bridge")
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
        self._set_bridge_interfaces()
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

        # The IP record may exist in IPAM without being assigned to any
        # interface (bare IP) — e.g., partially-enrolled DPU mgmt addresses,
        # or LLDP neighbors that happen to be hosts/DPUs whose mgmt-ip is
        # tracked but not as a switch interface. In that case
        # `assigned_object` is None and `.device` raises AttributeError.
        # Treat it the same as "not associated to a Netbox Switch Device".
        if nb_mgmt_ip.assigned_object is None:
            logging.error(
                "Switch IP {} is found but not assigned to any interface in Netbox".format(
                    switch_ip
                )
            )
            return nb_server_interface

        try:
            nb_switch = nb_mgmt_ip.assigned_object.device
            logging.info(
                "Found a switch in Netbox based on LLDP infos: {} (id: {})".format(
                    switch_ip, nb_switch.id
                )
            )
        except (KeyError, AttributeError):
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
        # Defensive: the switch interface may already have a cable that the
        # server-side check at create_or_update_cable() didn't see. Two cases:
        #   (a) Orphan cable (one side missing terminations) — delete it and
        #       proceed to create a fresh, fully-terminated cable.
        #   (b) Valid cable to a different host — log error and return without
        #       creating. NetBox would 400 on a duplicate termination otherwise,
        #       crashing the whole agent run.
        # Refetch via nb.dcim.cables.get() to be sure terminations are loaded.
        existing_ref = nb_switch_interface.cable
        if existing_ref is not None:
            existing = nb.dcim.cables.get(existing_ref.id)
            a_term = list(existing.a_terminations or [])
            b_term = list(existing.b_terminations or [])
            if not a_term or not b_term:
                logging.warning(
                    "Switch interface {sw_iface} (id={sw_iface_id}) on {sw_ip} "
                    "has orphaned cable {cable_id} "
                    "(a_terminations={a}, b_terminations={b}) — deleting before recable".format(
                        sw_iface=switch_interface,
                        sw_iface_id=nb_switch_interface.id,
                        sw_ip=switch_ip,
                        cable_id=existing.id,
                        a=len(a_term),
                        b=len(b_term),
                    )
                )
                existing.delete()
            else:
                logging.error(
                    "Switch interface {sw_iface} on {sw_ip} is already cabled "
                    "(cable {cable_id}) to another fully-terminated endpoint — "
                    "skipping recable from {host_iface} to avoid NetBox 400 "
                    "duplicate-termination crash. Resolve the conflict manually.".format(
                        sw_iface=switch_interface,
                        sw_ip=switch_ip,
                        cable_id=existing.id,
                        host_iface=nb_server_interface.name,
                    )
                )
                return nb_server_interface
        try:
            cable = nb.dcim.cables.create(
                a_terminations=[
                    {"object_type": "dcim.interface", "object_id": nb_server_interface.id},
                ],
                b_terminations=[
                    {"object_type": "dcim.interface", "object_id": nb_switch_interface.id},
                ],
            )
        except RequestError as exc:
            # Cabling is enrichment; inventory is the product. A cable NetBox
            # refuses must never abort the run — the exception used to unwind
            # all the way out of netbox_create_or_update, discarding every
            # module, interface and IP already gathered, and leaving the device
            # with no heartbeat at all.
            #
            # Most common cause: NetBox rejects a termination whose interface
            # type is in NONCONNECTABLE_IFACE_TYPES — virtual + wireless
            # (dcim/models/cables.py:574, dcim/constants.py:74) — with
            # 400 "Cables cannot be terminated to Virtual interfaces". A VLAN
            # subinterface such as `1s0f0.234` is the usual trigger.
            logging.error(
                "Failed to cable {interface} to {switch_interface} on {switch_ip}: "
                "{err} — continuing sync without this cable".format(
                    interface=nb_server_interface.name,
                    switch_interface=switch_interface,
                    switch_ip=switch_ip,
                    err=exc,
                )
            )
            return nb_server_interface
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
            # Verify the existing cable still points to the LLDP-reported
            # switch + port. We already have switch_ip from LLDP — look it
            # up in IPAM directly and confirm it's assigned to an iface on
            # the cable's far-side switch. Avoids the previous code's
            # `nb.dcim.interfaces.get(mgmt_only=True)` which raised ValueError
            # on switches with more than one mgmt_only iface (e.g., Arista's
            # Management + Management1).
            nb_sw_int = nb_server_interface.cable.b_terminations[0]
            nb_sw = nb_sw_int.device

            # Look up the LLDP-reported IP in IPAM. CIDR notation in NetBox.
            lldp_ip_objs = list(nb.ipam.ip_addresses.filter(address=switch_ip))
            cabled_to_correct_switch = False
            for ip_obj in lldp_ip_objs:
                if ip_obj.assigned_object_type != "dcim.interface":
                    continue
                assigned_iface = nb.dcim.interfaces.get(ip_obj.assigned_object_id)
                if assigned_iface and assigned_iface.device.id == nb_sw.id:
                    cabled_to_correct_switch = True
                    break

            if not cabled_to_correct_switch or nb_sw_int.name != switch_interface:
                logging.info("Netbox cable is not connected to correct ports, fixing..")
                logging.info(
                    "Deleting cable {cable_id} from {interface} to {switch_interface} of "
                    "{switch_ip}".format(
                        cable_id=nb_server_interface.cable.id,
                        interface=nb_server_interface.name,
                        switch_interface=nb_sw_int.name,
                        switch_ip=switch_ip,
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

import json
import re
import subprocess
import logging
import socket
import sys
from datetime import datetime, timezone

import netbox_agent.dmidecode as dmidecode
from netbox_agent.config import config
from netbox_agent.config import netbox_instance as nb
from netbox_agent.dependencies import missing_deps_string
from netbox_agent.hypervisor import Hypervisor
from netbox_agent.inventory import Inventory
from netbox_agent.location import Datacenter, Rack, Tenant
from netbox_agent.misc import (
    create_netbox_tags,
    get_device_role,
    get_device_type,
    get_device_platform,
    get_or_create_manufacturer,
)
from netbox_agent.network import ServerNetwork
from netbox_agent.power import PowerSupply
from pprint import pprint

# Base-36 asset tag validation: 4-char alphanumeric
_ASSET_TAG_RE = re.compile(r"^[0-9A-Z]{4}$", re.IGNORECASE)
_ASSET_TAG_PLACEHOLDERS = {
    "Not Specified", "None", "N/A", "To Be Filled By O.E.M.", "",
    "Chassis Asset Tag", "Default string", "No Asset Tag",
}


class ServerBase:
    def __init__(self, dmi=None):
        if dmi:
            self.dmi = dmi
        else:
            self.dmi = dmidecode.parse()

        self.baseboard = dmidecode.get_by_type(self.dmi, "Baseboard")
        self.bios = dmidecode.get_by_type(self.dmi, "BIOS")
        self.chassis = dmidecode.get_by_type(self.dmi, "Chassis")
        self.system = dmidecode.get_by_type(self.dmi, "System")
        self.device_platform = get_device_platform(config.device.platform)

        self.network = None

        self.tags = (
            list(set([x.strip() for x in config.device.tags.split(",") if x.strip()]))
            if config.device.tags
            else []
        )
        self.nb_tags = list(create_netbox_tags(self.tags))
        config_cf = set([f.strip() for f in config.device.custom_fields.split(",") if f.strip()])
        self.custom_fields = {}
        self.custom_fields.update(
            dict([(k.strip(), v.strip()) for k, v in [f.split("=", 1) for f in config_cf]])
        )

    def get_tenant(self):
        tenant = Tenant()
        return tenant.get()

    def get_netbox_tenant(self):
        tenant = self.get_tenant()
        if tenant is None:
            return None
        nb_tenant = nb.tenancy.tenants.get(slug=self.get_tenant())
        return nb_tenant

    def get_datacenter(self):
        dc = Datacenter()
        return dc.get()

    def get_netbox_datacenter(self):
        dc = self.get_datacenter()
        if dc is None:
            logging.error("Specifying a datacenter (Site) is mandatory in Netbox")
            sys.exit(1)

        nb_dc = nb.dcim.sites.get(
            slug=dc,
        )
        if nb_dc is None:
            logging.error("Site (slug: {}) has not been found".format(dc))
            sys.exit(1)

        return nb_dc

    def update_netbox_location(self, server):
        dc = self.get_datacenter()
        nb_rack = self.get_netbox_rack()
        nb_dc = self.get_netbox_datacenter()

        update = False
        if dc and server.site and server.site.slug != nb_dc.slug:
            logging.info(
                "Datacenter location has changed from {} to {}, updating".format(
                    server.site.slug,
                    nb_dc.slug,
                )
            )
            update = True
            server.site = nb_dc.id

        if server.rack and nb_rack and server.rack.id != nb_rack.id:
            logging.info(
                "Rack location has changed from {} to {}, updating".format(
                    server.rack,
                    nb_rack,
                )
            )
            update = True
            server.rack = nb_rack
            if nb_rack is None:
                server.face = None
                server.position = None
        return update, server

    def update_netbox_expansion_location(self, server, expansion):
        update = False
        if expansion.tenant != server.tenant:
            expansion.tenant = server.tenant
            update = True
        if expansion.site != server.site:
            expansion.site = server.site
            update = True
        if expansion.rack != server.rack:
            expansion.rack = server.rack
            update = True
        return update

    def get_rack(self):
        rack = Rack()
        return rack.get()

    def get_netbox_rack(self):
        rack = self.get_rack()
        datacenter = self.get_netbox_datacenter()
        if not rack:
            return None
        if rack and not datacenter:
            logging.error("Can't get rack if no datacenter is configured or found")
            sys.exit(1)

        return nb.dcim.racks.get(
            name=rack,
            site_id=datacenter.id,
        )

    def get_manufacturer(self):
        """
        Return the system manufacturer from dmidecode info (e.g. 'Supermicro').
        """
        try:
            return self.system[0]["Manufacturer"].strip()
        except (IndexError, KeyError):
            return None

    def get_product_name(self):
        """
        Return the Chassis Name from dmidecode info
        """
        return self.system[0]["Product Name"].strip()

    def get_service_tag(self):
        """
        Return the Service Tag from dmidecode info
        """
        return self.system[0]["Serial Number"].strip()

    def get_expansion_service_tag(self):
        """
        Return the virtual Service Tag from dmidecode info host
        with 'expansion'
        """
        return self.system[0]["Serial Number"].strip() + " expansion"

    def get_hostname(self):
        if config.hostname_cmd is None:
            return "{}".format(socket.gethostname())
        return subprocess.getoutput(config.hostname_cmd)

    def is_blade(self):
        raise NotImplementedError

    def get_blade_slot(self):
        raise NotImplementedError

    def get_chassis(self):
        raise NotImplementedError

    def get_chassis_name(self):
        raise NotImplementedError

    def get_chassis_service_tag(self):
        raise NotImplementedError

    def get_bios_version(self):
        raise NotImplementedError

    def get_bios_version_attr(self):
        raise NotImplementedError

    def get_bios_release_date(self):
        raise NotImplementedError

    def get_power_consumption(self):
        raise NotImplementedError

    def get_expansion_product(self):
        raise NotImplementedError

    def _netbox_create_chassis(self, datacenter, tenant, rack):
        device_type = get_device_type(self.get_chassis(), manufacturer=self.get_manufacturer())
        device_role = get_device_role(config.device.chassis_role)
        serial = self.get_chassis_service_tag()
        logging.info("Creating chassis blade (serial: {serial})".format(serial=serial))
        new_chassis = nb.dcim.devices.create(
            name=self.get_chassis_name(),
            device_type=device_type.id,
            serial=serial,
            role=device_role.id,
            site=datacenter.id if datacenter else None,
            tenant=tenant.id if tenant else None,
            rack=rack.id if rack else None,
            status="active",
            tags=[{"name": x} for x in self.tags],
            custom_fields=self.custom_fields,
        )
        return new_chassis

    def _netbox_create_blade(self, chassis, datacenter, tenant, rack):
        device_role = get_device_role(config.device.blade_role)
        device_type = get_device_type(self.get_product_name(), manufacturer=self.get_manufacturer())
        serial = self.get_service_tag()
        hostname = self.get_hostname()
        logging.info(
            "Creating blade (serial: {serial}) {hostname} on chassis {chassis_serial}".format(
                serial=serial, hostname=hostname, chassis_serial=chassis.serial
            )
        )
        new_blade = nb.dcim.devices.create(
            name=hostname,
            serial=serial,
            role=device_role.id,
            device_type=device_type.id,
            parent_device=chassis.id,
            site=datacenter.id if datacenter else None,
            tenant=tenant.id if tenant else None,
            rack=rack.id if rack else None,
            status="active",
            tags=[{"name": x} for x in self.tags],
            custom_fields=self.custom_fields,
        )
        return new_blade

    def _netbox_create_blade_expansion(self, chassis, datacenter, tenant, rack):
        device_role = get_device_role(config.device.blade_role)
        device_type = get_device_type(self.get_expansion_product(), manufacturer=self.get_manufacturer())
        serial = self.get_expansion_service_tag()
        hostname = self.get_hostname() + " expansion"
        logging.info(
            "Creating expansion (serial: {serial}) {hostname} on chassis {chassis_serial}".format(
                serial=serial, hostname=hostname, chassis_serial=chassis.serial
            )
        )
        new_blade = nb.dcim.devices.create(
            name=hostname,
            serial=serial,
            role=device_role.id,
            device_type=device_type.id,
            parent_device=chassis.id,
            site=datacenter.id if datacenter else None,
            tenant=tenant.id if tenant else None,
            rack=rack.id if rack else None,
            status="active",
            tags=[{"name": x} for x in self.tags],
        )
        return new_blade

    def _netbox_deduplicate_server(self, purge):
        serial = self.get_service_tag()
        hostname = self.get_hostname()
        server = nb.dcim.devices.get(name=hostname)
        if server and server.serial != serial:
            if purge:
                server.delete()
            else:
                server.serial = serial
                server.save()

    # Roles that are auto-assigned and can be corrected by hardware detection.
    # Manually-set roles (Firewall, JBOF, etc.) are never touched.
    _AUTO_ASSIGNABLE_ROLES = {"Server", "GPU Server", "CPU Server", "Storage Server"}

    def _refine_role(self, server):
        """Assign or correct server role based on hardware detection.

        Runs on every sync to ensure the role stays accurate as hardware
        changes (GPUs added/removed, disks added/removed).

        Only modifies roles in _AUTO_ASSIGNABLE_ROLES. Manually-set roles
        like Firewall, JBOF, PDU, etc. are never touched.
        """
        current_role = server.role
        if not current_role:
            return

        role_name = current_role.name if hasattr(current_role, 'name') else str(current_role)
        if role_name not in self._AUTO_ASSIGNABLE_ROLES:
            return  # Manually set role — don't touch

        # Detect hardware to determine refined role
        new_role_name = self._detect_server_type()
        if new_role_name and new_role_name != "Server":
            role = nb.dcim.device_roles.get(name=new_role_name)
            if role:
                server.role = role.id
                server.save()
                logging.info(
                    "Refined role for '%s': Server → %s",
                    server.name, new_role_name,
                )
            else:
                logging.warning(
                    "Role '%s' not found in NetBox — skipping refinement for '%s'",
                    new_role_name, server.name,
                )

    # Vendors/keywords for filtering onboard VGA from real GPUs
    _SKIP_GPU_VENDORS = {"aspeed technology, inc.", "matrox electronics systems ltd."}
    _SKIP_GPU_KEYWORDS = {"aspeed", "matrox", "vga compatible"}

    # Minimum discrete GPUs to classify as "GPU Server".
    # Filters out machines with 1-2 GPUs used for display/monitoring.
    # Most GPU servers have 3+ (4x RTX 4090, 8x H100, 8x B200, etc.)
    _MIN_GPU_COUNT = 3

    # Minimum physical disk count to classify as "Storage Server".
    # Standard servers have 1-4 disks (OS + data). Storage servers
    # have 8+ (NVMe shelves, JBOF-connected, Ceph/Weka nodes).
    _MIN_STORAGE_DISK_COUNT = 6

    def _detect_server_type(self) -> str:
        """Determine server type from hardware.

        Thresholds:
          GPU Server:     ≥ 3 discrete GPUs (NVIDIA, AMD, Intel Gaudi)
          Storage Server: ≥ 6 physical disks
          CPU Server:     everything else

        GPU detection filters out onboard VGA (Aspeed, Matrox) and
        known non-GPU vendors. Only counts discrete GPUs from NVIDIA,
        AMD, or Intel (Gaudi).

        Storage detection counts physical block devices excluding
        virtual devices (loop, ram, zram, device-mapper).

        Returns: 'GPU Server', 'CPU Server', or 'Storage Server'.
        """
        # --- Check for discrete GPUs ---
        gpu_count = 0
        try:
            from netbox_agent.lshw import LSHW
            lshw = LSHW()
            gpus = lshw.get_hw_linux("gpu")

            # Known GPU vendors (discrete GPUs, not onboard VGA)
            _GPU_VENDORS = {"nvidia", "amd", "ati", "habana", "intel"}
            _SKIP_VENDORS = {"aspeed", "matrox"}

            for gpu in gpus:
                vendor = gpu.get("vendor", "").lower()
                product = gpu.get("product", "").lower()

                # Skip known onboard VGA
                if any(sv in vendor for sv in _SKIP_VENDORS):
                    continue
                if any(kw in product for kw in ("aspeed", "matrox")):
                    continue

                # Only count if vendor is a known GPU maker
                is_known_gpu = any(gv in vendor for gv in _GPU_VENDORS)
                if is_known_gpu:
                    gpu_count += 1
        except Exception as e:
            logging.warning("GPU detection failed during role refinement: %s", e)

        if gpu_count >= self._MIN_GPU_COUNT:
            return "GPU Server"

        # --- Check for bulk storage ---
        disk_count = 0
        try:
            output = subprocess.check_output(
                ["lsblk", "-J", "-b", "-d", "-o", "NAME,TYPE,SIZE"],
                encoding="utf-8", timeout=10,
            )
            data = json.loads(output)
            disks = [d for d in data.get("blockdevices", [])
                     if d.get("type") == "disk"
                     and not d.get("name", "").startswith(
                         ("loop", "ram", "zram", "dm-", "md")
                     )]
            disk_count = len(disks)
        except Exception as e:
            logging.warning("Storage detection failed during role refinement: %s", e)

        if disk_count >= self._MIN_STORAGE_DISK_COUNT:
            return "Storage Server"

        # Default: CPU Server
        return "CPU Server"

    # --- Tenant auto-detection from running services ---

    # Services whose presence indicates RunPod tenant
    _RUNPOD_SERVICES = ("runpod", "safe_runpod", "runpod-worker")
    # MooseFS services indicate dedicated storage for RunPod
    _MOOSEFS_SERVICES = (
        "moosefs-chunkserver", "moosefs-master", "moosefs-metalogger",
        "mfschunkserver", "mfsmaster",
    )

    def _detect_tenant(self) -> str:
        """Detect tenant from running systemd services.

        Returns tenant slug:
          - 'runpod' if any RunPod or MooseFS service is active
          - 'farmgpu' otherwise
        """
        for svc in self._RUNPOD_SERVICES + self._MOOSEFS_SERVICES:
            try:
                result = subprocess.run(
                    ["systemctl", "is-active", svc],
                    capture_output=True, encoding="utf-8", timeout=5,
                )
                if result.stdout.strip() == "active":
                    logging.debug("Tenant detection: service '%s' is active → runpod", svc)
                    return "runpod"
            except Exception:
                pass
        return "farmgpu"

    def _sync_tenant(self, server):
        """Sync tenant based on detected running services.

        Updates tenant on every sync cycle so repurposed machines
        auto-flip between RunPod and FarmGPU.
        """
        tenant_slug = self._detect_tenant()
        nb_tenant = nb.tenancy.tenants.get(slug=tenant_slug)
        if not nb_tenant:
            logging.warning(
                "Tenant '%s' not found in NetBox — skipping tenant sync for '%s'",
                tenant_slug, server.name,
            )
            return

        current_tenant_id = server.tenant.id if server.tenant else None
        if current_tenant_id != nb_tenant.id:
            old_name = server.tenant.name if server.tenant else "(none)"
            server.tenant = nb_tenant.id
            server.save()
            logging.info(
                "Tenant for '%s': %s → %s",
                server.name, old_name, nb_tenant.name,
            )

    def _netbox_create_server(self, datacenter, tenant, rack):
        device_role = get_device_role(config.device.server_role)
        device_type = get_device_type(self.get_product_name(), manufacturer=self.get_manufacturer())
        if not device_type:
            raise Exception('Chassis "{}" doesn\'t exist'.format(self.get_chassis()))
        serial = self.get_service_tag()
        hostname = self.get_hostname()
        logging.info(
            "Creating server (serial: {serial}) {hostname}".format(
                serial=serial, hostname=hostname
            )
        )

        # Build custom fields with defaults for new fields
        cf = dict(self.custom_fields)
        default_owner = getattr(config.device, "default_owner", "FarmGPU")
        cf.setdefault("owner", default_owner)
        cf.setdefault("environment", "Production")
        cf.setdefault("record_completeness", "incomplete")

        # Include BMC MAC and chassis serial at creation time
        bmc_mac = self._get_bmc_mac()
        if bmc_mac:
            cf["bmc_mac_address"] = bmc_mac
        chassis_serial = self._get_chassis_serial()
        if chassis_serial:
            cf["chassis_serial"] = chassis_serial

        # Set last_agent_sync at creation time
        cf["last_agent_sync"] = datetime.now(timezone.utc).isoformat()

        create_kwargs = dict(
            name=hostname,
            serial=serial,
            role=device_role.id,
            device_type=device_type.id,
            platform=self.device_platform.id,
            site=datacenter.id if datacenter else None,
            tenant=tenant.id if tenant else None,
            rack=rack.id if rack else None,
            status="active",
            tags=[{"name": x} for x in self.tags],
            custom_fields=cf,
        )

        # Set asset tag if available
        asset_tag = self.get_asset_tag()
        if asset_tag:
            create_kwargs["asset_tag"] = asset_tag

        new_server = nb.dcim.devices.create(**create_kwargs)
        return new_server

    def _ensure_required_custom_fields(self, server, config):
        """
        Ensure required custom fields have values on existing devices.
        NetBox validates ALL required CFs on any PATCH, so we must fill
        missing ones before any save() call succeeds.
        """
        cf = dict(server.custom_fields or {})
        changed = False
        default_owner = getattr(config.device, "default_owner", "FarmGPU")

        if not cf.get("owner"):
            cf["owner"] = default_owner
            changed = True
        if not cf.get("environment"):
            cf["environment"] = "Production"
            changed = True
        if not cf.get("record_completeness"):
            cf["record_completeness"] = "incomplete"
            changed = True

        if changed:
            logging.info(
                "Backfilling required custom fields on '%s': %s",
                server.name,
                {k: cf[k] for k in ("owner", "environment", "record_completeness")},
            )
            server.custom_fields = cf
            server.save()

    def get_asset_tag(self):
        """
        Read asset tag from config command, IPMI FRU, or DMI chassis.
        Returns validated Base-36 tag string (4 chars, 0-9/A-Z) or None.
        """
        tag = None

        # Source 1: Config command (highest priority)
        asset_tag_cmd = getattr(config.device, "asset_tag_cmd", None)
        if asset_tag_cmd:
            try:
                tag = subprocess.getoutput(asset_tag_cmd).strip()
            except Exception:
                tag = None

        # Source 2: IPMI FRU "Product Asset Tag" (most reliable on Supermicro)
        if not tag or tag in _ASSET_TAG_PLACEHOLDERS or not _ASSET_TAG_RE.match(tag):
            try:
                output = subprocess.check_output(
                    ["ipmitool", "fru", "print", "0"],
                    encoding="utf-8", timeout=10, stderr=subprocess.DEVNULL,
                )
                for line in output.splitlines():
                    if "Product Asset Tag" in line and ":" in line:
                        tag = line.split(":", 1)[1].strip()
                        break
            except Exception:
                pass

        # Source 3: DMI Chassis Asset Tag (fallback)
        if not tag or tag in _ASSET_TAG_PLACEHOLDERS or not _ASSET_TAG_RE.match(tag):
            if self.chassis:
                tag = self.chassis[0].get("Asset Tag", "").strip()

        # Validate: must be exactly 4 alphanumeric chars, not a placeholder
        if tag and tag not in _ASSET_TAG_PLACEHOLDERS and _ASSET_TAG_RE.match(tag):
            return tag.upper()
        return None

    def get_netbox_server(self, expansion=False):
        """
        Triple-mode device lookup: asset_tag → serial → BMC MAC.

        BMC API creates device skeletons with BMC MAC + OOB IP.  Some devices
        (e.g. Gigabyte) have no usable serial and the asset tag may not yet be
        programmed.  Matching by BMC MAC ensures netbox-agent enriches the
        existing skeleton rather than creating a duplicate.
        """
        if expansion:
            return nb.dcim.devices.get(serial=self.get_expansion_service_tag())

        # Try asset tag first (case-insensitive — BMC may report 101K vs 101k)
        asset_tag = self.get_asset_tag()
        if asset_tag:
            device = nb.dcim.devices.get(asset_tag=asset_tag)
            if not device:
                # Try lowercase — BMC API stores lowercase, DMI may report uppercase
                device = nb.dcim.devices.get(asset_tag=asset_tag.lower())
            if device:
                return device
            logging.debug("No device found with asset_tag=%s, falling back to serial", asset_tag)

        # Fall back to serial
        serial = self.get_service_tag()
        if serial:
            device = nb.dcim.devices.get(serial=serial)
            if device:
                return device
            logging.debug("No device found with serial=%s, falling back to BMC MAC", serial)

        # Fall back to BMC MAC (custom field cf_bmc_mac_address)
        bmc_mac = self._get_bmc_mac()
        if bmc_mac:
            devices = list(nb.dcim.devices.filter(cf_bmc_mac_address=bmc_mac))
            if devices:
                logging.info(
                    "Matched device by BMC MAC %s → %s (id=%s)",
                    bmc_mac, devices[0].name, devices[0].id,
                )
                return devices[0]
            logging.debug("No device found with bmc_mac=%s", bmc_mac)

        return None

    def _netbox_set_or_update_blade_slot(self, server, chassis, datacenter):
        # before everything check if right chassis
        actual_device_bay = server.parent_device.device_bay if server.parent_device else None
        actual_chassis = actual_device_bay.device if actual_device_bay else None
        slot = self.get_blade_slot()
        if (
            actual_chassis
            and actual_chassis.serial == chassis.serial
            and actual_device_bay.name == slot
        ):
            return

        real_device_bays = nb.dcim.device_bays.filter(
            device_id=chassis.id,
            name=slot,
        )
        real_device_bays = nb.dcim.device_bays.filter(
            device_id=chassis.id,
            name=slot,
        )
        if real_device_bays:
            logging.info(
                "Setting device ({serial}) new slot on {slot} (Chassis {chassis_serial})..".format(
                    serial=server.serial, slot=slot, chassis_serial=chassis.serial
                )
            )
            # reset actual device bay if set
            if actual_device_bay:
                # Forces the evaluation of the installed_device attribute to
                # workaround a bug probably due to lazy loading optimization
                # that prevents the value change detection
                actual_device_bay.installed_device
                actual_device_bay.installed_device = None
                actual_device_bay.save()
            # setup new device bay
            real_device_bay = next(real_device_bays)
            real_device_bay.installed_device = server
            real_device_bay.save()
        else:
            logging.error("Could not find slot {slot} for chassis".format(slot=slot))

    def _netbox_set_or_update_blade_expansion_slot(self, expansion, chassis, datacenter):
        # before everything check if right chassis
        actual_device_bay = expansion.parent_device.device_bay if expansion.parent_device else None
        actual_chassis = actual_device_bay.device if actual_device_bay else None
        slot = self.get_blade_expansion_slot()
        if (
            actual_chassis
            and actual_chassis.serial == chassis.serial
            and actual_device_bay.name == slot
        ):
            return

        real_device_bays = nb.dcim.device_bays.filter(
            device_id=chassis.id,
            name=slot,
        )
        if not real_device_bays:
            logging.error("Could not find slot {slot} expansion for chassis".format(slot=slot))
            return
        logging.info(
            "Setting device expansion ({serial}) new slot on {slot} "
            "(Chassis {chassis_serial})..".format(
                serial=expansion.serial, slot=slot, chassis_serial=chassis.serial
            )
        )
        # reset actual device bay if set
        if actual_device_bay:
            # Forces the evaluation of the installed_device attribute to
            # workaround a bug probably due to lazy loading optimization
            # that prevents the value change detection
            actual_device_bay.installed_device
            actual_device_bay.installed_device = None
            actual_device_bay.save()
        # setup new device bay
        real_device_bay = next(real_device_bays)
        real_device_bay.installed_device = expansion
        real_device_bay.save()

    def netbox_create_or_update(self, config, deps=None, network_only=False, state=None):
        """
        Netbox method to create or update info about our server/blade

        Handle:
        * new chassis for a blade
        * new slot for a blade
        * hostname update
        * Network infos
        * Inventory management
        * PSU management
        * virtualization cluster device

        Args:
            config: Parsed configuration namespace
            deps: dict of {tool_name: bool} from dependencies.check_all()
            network_only: If True, skip hardware sync — only update network
            state: StateManager instance for diff-based sync
        """
        datacenter = self.get_netbox_datacenter()
        rack = self.get_netbox_rack()
        tenant = self.get_netbox_tenant()

        if config.update_old_devices:
            self._netbox_deduplicate_server(purge=False)

        if config.purge_old_devices:
            self._netbox_deduplicate_server(purge=True)

        if self.is_blade():
            chassis = nb.dcim.devices.get(serial=self.get_chassis_service_tag())
            # Chassis does not exist
            if not chassis:
                chassis = self._netbox_create_chassis(datacenter, tenant, rack)

            server = self.get_netbox_server()
            if not server:
                server = self._netbox_create_blade(chassis, datacenter, tenant, rack)

            # Set slot for blade
            self._netbox_set_or_update_blade_slot(server, chassis, datacenter)
        else:
            server = self.get_netbox_server()
            if not server:
                server = self._netbox_create_server(datacenter, tenant, rack)

        # Ensure required custom fields are populated on existing devices.
        # NetBox validates ALL required CFs on any PATCH, so we must fill
        # them before saving any field (e.g., asset_tag).
        self._ensure_required_custom_fields(server, config)

        # Record missing dependencies as a custom field on the device
        if deps is not None:
            missing_str = missing_deps_string(deps)
            cf = dict(server.custom_fields or {})
            if cf.get("missing_agent_dependencies") != missing_str:
                cf["missing_agent_dependencies"] = missing_str
                server.custom_fields = cf
                server.save()
                if missing_str:
                    logging.info("Missing dependencies on '%s': %s", server.name, missing_str)
                server = nb.dcim.devices.get(server.id)  # re-fetch after save

        # Sync asset tag: only populate if NetBox record has NO asset tag.
        # BMC API is the authority for asset_tag (programmed via Redfish).
        # netbox-agent should never overwrite an existing tag — the OS-level
        # dmidecode value may differ from the Redfish-programmed value on
        # some platforms (e.g., AST2600 where FRU and Redfish AssetTag diverge).
        local_asset_tag = self.get_asset_tag()
        existing_tag = getattr(server, "asset_tag", None)
        if local_asset_tag and not existing_tag:
            logging.info(
                "Setting initial asset_tag on '%s': %s",
                server.name,
                local_asset_tag,
            )
            server.asset_tag = local_asset_tag
            server.save()
        elif local_asset_tag and existing_tag and local_asset_tag != existing_tag:
            logging.warning(
                "Asset tag mismatch on '%s': NetBox=%s, local=%s "
                "(keeping NetBox value — BMC API is authoritative)",
                server.name,
                existing_tag,
                local_asset_tag,
            )

        logging.debug("Updating Server...")
        # check network cards
        if config.register or config.update_all or config.update_network or network_only:
            self.network = ServerNetwork(server=self)
            self.network.create_or_update_netbox_network_cards()

        # Defaults for variables used later (expansion slot path)
        update_inventory = False

        # When network_only, skip all hardware sync
        if not network_only:
            update_inventory = config.inventory and (
                config.register or config.update_all or config.update_inventory
            )
            # update inventory if feature is enabled (legacy Inventory Items)
            if update_inventory:
                self.inventory = Inventory(server=self)
                self.inventory.create_or_update()
            # update modules if feature is enabled (new Modules API)
            update_modules = getattr(config, "modules", False) and (
                config.register or config.update_all or getattr(config, "update_modules", False)
            )
            if update_modules:
                from netbox_agent.modules import ModuleManager
                self.module_manager = ModuleManager(server=self, config=config)
                self.module_manager.create_or_update(deps=deps, state=state)
            # update psu
            if config.register or config.update_all or config.update_psu:
                self.power = PowerSupply(server=self)
                self.power.create_or_update_power_supply()
                self.power.report_power_consumption()
            # update virtualization cluster and virtual machines
            if config.virtual.hypervisor and (
                config.register or config.update_all or config.update_hypervisor
            ):
                self.hypervisor = Hypervisor(server=self)
                self.hypervisor.create_or_update_device_cluster()
                if config.virtual.list_guests_cmd:
                    self.hypervisor.create_or_update_device_virtual_machines()

        expansion = nb.dcim.devices.get(serial=self.get_expansion_service_tag())
        if self.own_expansion_slot() and config.expansion_as_device:
            logging.debug("Update Server expansion...")
            if not expansion:
                expansion = self._netbox_create_blade_expansion(chassis, datacenter, tenant, rack)

            # set slot for blade expansion
            self._netbox_set_or_update_blade_expansion_slot(expansion, chassis, datacenter)
            if update_inventory:
                # Updates expansion inventory
                inventory = Inventory(server=self, update_expansion=True)
                inventory.create_or_update()
        elif self.own_expansion_slot() and expansion:
            expansion.delete()
            expansion = None

        update = 0
        # for every other specs
        # check hostname
        if server.name != self.get_hostname():
            server.name = self.get_hostname()
            update += 1

        # Sync device serial — system serial only, no fallbacks
        local_serial = self._get_best_serial() or ""
        if server.serial != local_serial:
            logging.info(
                "Updating serial on '%s': %s -> %s",
                server.name, server.serial, local_serial or "(empty)",
            )
            server.serial = local_serial
            update += 1

        server_tags = sorted(set([x.name for x in server.tags]))
        tags = sorted(set(self.tags))
        if server_tags != tags:
            new_tags_ids = [x.id for x in self.nb_tags]
            if not config.preserve_tags:
                server.tags = new_tags_ids
            else:
                server_tags_ids = [x.id for x in server.tags]
                server.tags = sorted(set(new_tags_ids + server_tags_ids))
            update += 1

        # Populate chassis_serial and bmc_mac_address custom fields
        local_cf = dict(self.custom_fields)
        chassis_serial = self._get_chassis_serial()
        if chassis_serial:
            local_cf["chassis_serial"] = chassis_serial
        bmc_mac = self._get_bmc_mac()
        if bmc_mac:
            local_cf["bmc_mac_address"] = bmc_mac

        # Always update last_agent_sync timestamp on successful sync
        local_cf["last_agent_sync"] = datetime.now(timezone.utc).isoformat()

        if server.custom_fields != local_cf:
            server.custom_fields = local_cf
            update += 1

        # Transition device to "active" on successful agent sync.
        # Only transition from inventory/staged/offline — never override
        # manual statuses like failed or decommissioning.
        _ACTIVATABLE_STATUSES = {"inventory", "staged", "planned", "offline"}
        current_status = getattr(server, "status", None)
        # pynetbox returns status as a Record with .value attribute
        current_status_value = (
            current_status.value if hasattr(current_status, "value") else current_status
        )
        if current_status_value in _ACTIVATABLE_STATUSES:
            logging.info(
                "Transitioning device '%s' status: %s → active",
                server.name, current_status_value,
            )
            server.status = "active"
            update += 1

        if config.update_all or config.update_location:
            ret, server = self.update_netbox_location(server)
            update += ret

        if server.platform != self.device_platform:
            server.platform = self.device_platform
            update += 1

        if update:
            server.save()

        # Refine generic "Server" role based on detected hardware
        self._refine_role(server)

        # Sync tenant based on running services (runpod/moosefs → RunPod, else → FarmGPU)
        self._sync_tenant(server)

        if expansion:
            update = 0
            expansion_name = server.name + " expansion"
            if expansion.name != expansion_name:
                expansion.name = expansion_name
                update += 1
            if self.update_netbox_expansion_location(server, expansion):
                update += 1
            if update:
                expansion.save()

        # Re-fetch IPs after network updates (IPs may have been added/removed)
        myips = list(nb.ipam.ip_addresses.filter(device_id=server.id))
        # Build a set of currently assigned IP IDs for validation
        assigned_ip_ids = {ip.id for ip in myips}

        # Re-fetch the device to get current oob_ip/primary_ip4 state
        server = nb.dcim.devices.get(server.id)

        # --- OOB IP (IPMI) assignment --- saved separately to avoid atomic failure ---
        oob_update = False

        # Clear oob_ip if it points to an IP no longer assigned to this device
        if server.oob_ip and server.oob_ip.id not in assigned_ip_ids:
            logging.info(
                "Clearing stale oob_ip %s (no longer assigned to device)",
                server.oob_ip,
            )
            server.oob_ip = None
            oob_update = True

        # Set oob_ip to the IPMI interface IP
        if not oob_update:
            for ip in myips:
                if ip.assigned_object and ip.assigned_object.display == "IPMI" and ip != server.oob_ip:
                    server.oob_ip = ip.id
                    oob_update = True
                    break

        if oob_update:
            try:
                server.save()
                logging.info(
                    "Saved oob_ip for device %s (id=%s)",
                    server.name, server.id,
                )
            except Exception as e:
                logging.error(
                    "Failed to save oob_ip for device %s (id=%s): %s",
                    server.name, server.id, e,
                )

        # --- Primary IPv4 assignment --- saved separately to avoid atomic failure ---
        # Re-fetch device to get clean state after oob_ip save
        server = nb.dcim.devices.get(server.id)
        primary_update = False

        # Clear primary_ip4 if it points to an IP no longer assigned
        if server.primary_ip4 and server.primary_ip4.id not in assigned_ip_ids:
            logging.info(
                "Clearing stale primary_ip4 %s (no longer assigned to device)",
                server.primary_ip4,
            )
            server.primary_ip4 = None
            primary_update = True

        # Set primary_ip4 to the management IP (default gateway interface)
        if not server.primary_ip4:
            mgmt_iface = self._get_default_gateway_interface()
            if mgmt_iface:
                for ip in myips:
                    if (
                        ip.assigned_object
                        and ip.assigned_object.display == mgmt_iface
                        and ip.family
                        and ip.family.value == 4
                    ):
                        server.primary_ip4 = ip.id
                        primary_update = True
                        break

        if primary_update:
            try:
                server.save()
                logging.info(
                    "Saved primary_ip4 for device %s (id=%s)",
                    server.name, server.id,
                )
            except Exception as e:
                logging.error(
                    "Failed to save primary_ip4 for device %s (id=%s): %s",
                    server.name, server.id, e,
                )

        logging.debug("Finished updating Server!")

    # DMI placeholder values that should be treated as "no serial"
    _DMI_PLACEHOLDERS = {
        "", "none", "n/a", "na", "not specified", "not available",
        "not applicable", "to be filled by o.e.m.", "default string",
        "0123456789", "..................", "system serial number",
        "chassis serial number", "base board serial number",
        "default", "unknown", "unspecified", "no asset information",
        "empty", "xxxxxxxxxxxx", "0000000000", "____________",
    }

    def _is_valid_serial(self, value):
        """Check if a serial string is real (not a placeholder)."""
        if not value or not isinstance(value, str):
            return False
        cleaned = value.strip()
        if not cleaned or len(cleaned) < 2:
            return False
        if cleaned.lower() in self._DMI_PLACEHOLDERS:
            return False
        # Reject strings that are all the same character (e.g. "000000", "XXXX")
        if len(set(cleaned.replace("-", "").replace(" ", ""))) <= 1:
            return False
        return True

    def _get_best_serial(self):
        """
        Return the system serial from DMI, or None if unavailable.

        No fallback cascade — the system serial is the system serial.
        Baseboard/chassis serials are tracked as separate inventory items.
        Machines are primarily identified by asset tag, not serial.
        """
        try:
            tag = self.get_service_tag()
            if self._is_valid_serial(tag):
                return tag.strip()
        except Exception:
            pass

        logging.warning("No valid system serial found for this device")
        return None

    def _get_chassis_serial(self):
        """
        Return the chassis serial number from DMI data.
        Distinct from the system serial (service tag) on many servers.
        """
        _PLACEHOLDERS = {
            "", "none", "n/a", "not specified", "not available",
            "to be filled by o.e.m.", "default string", "0123456789",
            "..................",
        }
        try:
            if self.chassis:
                serial = self.chassis[0].get("Serial Number", "").strip()
                if serial and serial.lower() not in _PLACEHOLDERS:
                    return serial
        except (IndexError, KeyError, AttributeError):
            pass
        return None

    def _get_bmc_mac(self):
        """Return the BMC MAC address from IPMI, if available."""
        try:
            from netbox_agent.ipmi import IPMI
            ipmi_data = IPMI().parse()
            if ipmi_data and ipmi_data.get("mac"):
                return ipmi_data["mac"].upper()
        except Exception:
            pass
        return None

    def _get_default_gateway_interface(self):
        """
        Detect the management interface by finding the default route.
        Uses `ip -j route show default` (JSON output). The interface with
        the default gateway is the management interface — same concept as
        SILO's ansible_host (the IP used for SSH/management access).
        Returns the interface name (e.g. "ens4035f0np0") or None.
        """
        try:
            output = subprocess.check_output(
                ["ip", "-j", "route", "show", "default"],
                encoding="utf-8",
                timeout=10,
            )
            routes = json.loads(output)
            if routes and isinstance(routes, list):
                dev = routes[0].get("dev")
                if dev:
                    logging.debug("Default gateway interface: %s", dev)
                    return dev
        except Exception as e:
            logging.warning("Failed to detect default gateway interface: %s", e)
        return None

    def print_debug(self):
        self.network = ServerNetwork(server=self)
        print("Datacenter:", self.get_datacenter())
        print("Netbox Datacenter:", self.get_netbox_datacenter())
        print("Rack:", self.get_rack())
        print("Netbox Rack:", self.get_netbox_rack())
        print("Is blade:", self.is_blade())
        print("Got expansion:", self.own_expansion_slot())
        print("Product Name:", self.get_product_name())
        print("Platform:", self.device_platform)
        print("Chassis:", self.get_chassis())
        print("Chassis service tag:", self.get_chassis_service_tag())
        print("Service tag:", self.get_service_tag())
        print(
            "NIC:",
        )
        pprint(self.network.get_network_cards())
        pass

    def own_expansion_slot(self):
        """
        Indicates if the device hosts an expansion card
        """
        return False

    def own_gpu_expansion_slot(self):
        """
        Indicates if the device hosts a GPU expansion card
        """
        return False

    def own_drive_expansion_slot(self):
        """
        Indicates if the device hosts a drive expansion bay
        """
        return False

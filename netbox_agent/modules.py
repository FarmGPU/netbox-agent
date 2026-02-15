"""
ModuleManager — Manages hardware components as NetBox Modules.

Replaces inventory.py functionality by using the Modules API instead of
Inventory Items. Supports re-parenting, spare inventory tracking, and
module type auto-creation with typed profiles.
"""

import json
import logging
import re
import subprocess
import time

from netbox_agent.config import netbox_instance as nb
from netbox_agent.lshw import LSHW
from netbox_agent.misc import is_tool

logger = logging.getLogger("netbox_agent.modules")

# Categories mapped to their bay prefix and profile name
# Profile names must match the actual names in NetBox (created by script 04)
CATEGORIES = {
    "cpu": {"prefix": "CPU", "profile": "CPU"},
    "gpu": {"prefix": "GPU", "profile": "GPU"},
    "dimm": {"prefix": "DIMM", "profile": "Memory"},
    "ssd": {"prefix": "SSD", "profile": "Hard disk"},
    "nic": {"prefix": "NIC", "profile": "NIC"},
    "psu": {"prefix": "PSU", "profile": "Power supply"},
}

# Retry settings for API calls
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds, doubles each retry


def _api_retry(func, *args, **kwargs):
    """Execute an API call with exponential backoff retry."""
    for attempt in range(MAX_RETRIES):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF * (2 ** attempt)
                logger.warning("API call failed (%s), retrying in %ds...", e, wait)
                time.sleep(wait)
            else:
                raise


class ModuleManager:
    """
    Manages hardware modules for a single device.

    Detects local hardware via lshw/dmidecode/nvidia-smi, resolves module types,
    ensures module bays exist, and syncs the state to NetBox.
    """

    def __init__(self, server, config):
        """
        Args:
            server: ServerBase instance (provides device info, dmi data)
            config: Parsed configuration namespace
        """
        self.server = server
        self.config = config
        self.lshw = LSHW()
        self.device = None  # NetBox device record, set during sync
        self.default_owner = getattr(config.device, "default_owner", "FarmGPU")

        # Caches to reduce API calls
        self._profile_cache = {}
        self._manufacturer_cache = {}
        self._module_type_cache = {}
        self._spare_device = None

    # ------------------------------------------------------------------ #
    #  Hardware Detection
    # ------------------------------------------------------------------ #

    # Co-processor / accelerator keywords that lshw reports as class "processor"
    # but are NOT physical CPUs (e.g., Intel QAT, DLB, IAA).
    _SKIP_CPU_KEYWORDS = {
        "quickassist", "qat", "dlb", "iaa", "dsa",
        "co-processor", "coprocessor", "accelerator",
    }

    def _get_local_cpus(self):
        """Detect CPUs via lshw. Filters out QAT and other co-processors."""
        items = []
        for cpu in self.lshw.get_hw_linux("cpu"):
            product = cpu.get("product", "Unknown CPU")
            description = cpu.get("description", "")

            # Skip co-processors: Intel QAT, DLB, IAA etc. show as class=processor
            combined = f"{product} {description}".lower()
            if any(kw in combined for kw in self._SKIP_CPU_KEYWORDS):
                logger.debug("Skipping co-processor: %s (%s)", product, description)
                continue

            items.append({
                "product": product,
                "vendor": cpu.get("vendor", "Unknown"),
                "serial": None,  # CPUs rarely report serials
                "slot": cpu.get("location", ""),
            })
        return items

    # BMC/onboard VGA controllers that should NOT be tracked as GPU modules
    _SKIP_GPU_VENDORS = {"aspeed technology, inc.", "matrox electronics systems ltd."}
    _SKIP_GPU_KEYWORDS = {"aspeed", "matrox", "vga compatible"}

    def _get_local_gpus(self):
        """Detect GPUs via lshw + nvidia-smi for serials. Filters out BMC VGA controllers."""
        gpus = self.lshw.get_hw_linux("gpu")
        serials = self._get_nvidia_serials()
        items = []
        real_idx = 0  # index into nvidia-smi serials (only real GPUs)
        for gpu in gpus:
            product = gpu.get("product", "Unknown GPU")
            vendor = gpu.get("vendor", "Unknown")
            description = gpu.get("description", "")

            # Skip BMC/onboard VGA controllers
            if vendor.lower() in self._SKIP_GPU_VENDORS:
                logger.debug("Skipping onboard VGA: %s %s", vendor, product)
                continue
            if any(kw in product.lower() for kw in self._SKIP_GPU_KEYWORDS):
                logger.debug("Skipping onboard VGA: %s", product)
                continue
            # Skip if description says "VGA compatible" and not "3D" (onboard vs discrete)
            if "VGA compatible" in description and "3D" not in description:
                logger.debug("Skipping VGA-only device: %s %s", vendor, product)
                continue

            # Truncate long product names
            if len(product) > 50:
                product = product[:48] + ".."
            serial = serials.get(real_idx)
            items.append({
                "product": product,
                "vendor": vendor,
                "serial": serial,
                "description": description,
            })
            real_idx += 1
        return items

    def _get_nvidia_serials(self):
        """Query nvidia-smi for GPU serial numbers. Returns {index: serial}."""
        serials = {}
        if not is_tool("nvidia-smi"):
            return serials
        try:
            output = subprocess.check_output(
                ["nvidia-smi", "--query-gpu=index,serial", "--format=csv,noheader,nounits"],
                encoding="utf-8",
                timeout=30,
            ).strip()
            for line in output.splitlines():
                parts = line.split(",")
                if len(parts) == 2:
                    idx = int(parts[0].strip())
                    sn = parts[1].strip()
                    if sn and sn not in ("[N/A]", "N/A", "0", ""):
                        serials[idx] = sn
        except Exception as e:
            logger.warning("nvidia-smi serial query failed: %s", e)
        return serials

    def _get_local_dimms(self):
        """Detect DIMMs via lshw memory children."""
        items = []
        for dimm in self.lshw.memories:
            serial = dimm.get("serial", "N/A")
            if serial in ("N/A", "NO DIMM", "Not Specified", "Unknown"):
                serial = None
            size_gb = dimm.get("size", 0)
            if isinstance(size_gb, (int, float)):
                size_gb = int(size_gb)
            product = dimm.get("product", "Unknown")
            items.append({
                "product": f"{product} {size_gb}GB" if size_gb else product,
                "vendor": dimm.get("vendor", "Unknown"),
                "serial": serial,
                "slot": dimm.get("slot", ""),
                "size_gb": size_gb,
                "description": dimm.get("description", ""),
            })
        return items

    def _get_local_ssds(self):
        """Detect SSDs via lshw storage + nvme-cli + RAID tools."""
        items = []
        seen_serials = set()

        for disk in self.lshw.get_hw_linux("storage"):
            serial = disk.get("serial")
            product = disk.get("product")
            if not product:
                continue
            # Skip virtual/logical drives
            desc = (disk.get("description") or "").lower()
            if any(kw in desc for kw in ("volume", "virtual", "dvd-ram", "logical")):
                continue
            if serial and serial in seen_serials:
                continue
            if serial:
                seen_serials.add(serial)

            vendor = disk.get("vendor")
            if not vendor and product:
                vendor = self._guess_vendor(product)

            items.append({
                "product": product,
                "vendor": vendor or "Unknown",
                "serial": serial,
                "description": disk.get("description", ""),
            })

        return items

    def _get_local_nics(self):
        """
        Detect physical NICs via lshw, grouped by card (using product+vendor).
        Uses MAC address as a serial proxy.
        """
        items = []
        seen_macs = set()

        for iface in self.lshw.interfaces:
            mac = iface.get("serial", iface.get("macaddress", ""))
            product = iface.get("product", "Unknown NIC")
            if not mac or mac in seen_macs:
                continue
            seen_macs.add(mac)

            items.append({
                "product": product,
                "vendor": iface.get("vendor", "Unknown"),
                "serial": mac,  # MAC as serial proxy
                "description": iface.get("description", ""),
                "name": iface.get("name", ""),
            })

        return items

    def _get_local_psus(self):
        """Detect PSUs via dmidecode type 39."""
        items = []
        try:
            from netbox_agent import dmidecode
            dmi = self.server.dmi
            # Use numeric type ID 39 because _str2type has " Power Supply"
            # (with leading space), causing string lookup to fail.
            psus = dmidecode.get_by_type(dmi, 39) or []
            for psu in psus:
                name = psu.get("Name", "Unknown PSU")
                serial = psu.get("Serial Number", "")
                manufacturer = psu.get("Manufacturer", "Unknown")
                if serial in ("", "Not Specified", "To Be Filled By O.E.M.", "N/A"):
                    serial = None
                if name in ("Not Specified", "To Be Filled By O.E.M."):
                    name = "Unknown PSU"
                items.append({
                    "product": f"{manufacturer} {name}".strip(),
                    "vendor": manufacturer,
                    "serial": serial,
                    "description": "Power Supply",
                })
        except Exception as e:
            logger.warning("PSU detection failed: %s", e)

        return items

    def _guess_vendor(self, product):
        """Guess vendor from product name keywords."""
        product_lower = product.lower()
        vendors = {
            "samsung": "Samsung",
            "intel": "Intel",
            "solidigm": "Solidigm",
            "micron": "Micron",
            "western": "Western Digital",
            "seagate": "Seagate",
            "toshiba": "Toshiba",
            "hynix": "SK Hynix",
            "kingston": "Kingston",
            "crucial": "Crucial",
        }
        for keyword, name in vendors.items():
            if keyword in product_lower:
                return name
        return None

    # ------------------------------------------------------------------ #
    #  Module Type Resolution
    # ------------------------------------------------------------------ #

    def _get_profile(self, profile_name):
        """Get a module type profile by name (cached)."""
        if profile_name in self._profile_cache:
            return self._profile_cache[profile_name]
        profile = _api_retry(nb.dcim.module_type_profiles.get, name=profile_name)
        if profile:
            self._profile_cache[profile_name] = profile
        return profile

    def _get_or_create_manufacturer(self, name):
        """Find or create a manufacturer (cached)."""
        if not name or name in ("Unknown", "N/A"):
            name = "Unknown"
        if name in self._manufacturer_cache:
            return self._manufacturer_cache[name]

        mfr = _api_retry(nb.dcim.manufacturers.get, name=name)
        if not mfr:
            slug = re.sub(r"[^A-Za-z0-9]+", "-", name).lower().strip("-")
            if not slug:
                slug = "unknown"
            # Slug-based fallback (handles casing differences like "Broadcom" vs "BROADCOM")
            mfr = _api_retry(nb.dcim.manufacturers.get, slug=slug)
            if not mfr:
                mfr = _api_retry(nb.dcim.manufacturers.create, name=name, slug=slug)
                logger.info("Created manufacturer '%s'", name)
        self._manufacturer_cache[name] = mfr
        return mfr

    def _resolve_module_type(self, category, item):
        """
        Find or auto-create a ModuleType for the given hardware item.

        Args:
            category: One of 'cpu', 'gpu', 'dimm', 'ssd', 'nic', 'psu'
            item: dict with at least 'product' and 'vendor'

        Returns:
            pynetbox ModuleType record
        """
        product = item["product"]
        vendor = item.get("vendor", "Unknown")
        cache_key = f"{vendor}::{product}"

        if cache_key in self._module_type_cache:
            return self._module_type_cache[cache_key]

        mfr = self._get_or_create_manufacturer(vendor)
        profile_name = CATEGORIES[category]["profile"]
        profile = self._get_profile(profile_name)

        # Try to find existing module type
        mt = _api_retry(nb.dcim.module_types.get, manufacturer_id=mfr.id, model=product)
        if mt:
            self._module_type_cache[cache_key] = mt
            return mt

        # Auto-create with profile (no attribute_data — admin fills in later)
        create_params = {
            "manufacturer": mfr.id,
            "model": product,
        }
        if profile:
            create_params["profile"] = profile.id

        mt = _api_retry(nb.dcim.module_types.create, create_params)
        logger.info("Auto-created module type '%s / %s' (profile=%s)", vendor, product, profile_name)
        self._module_type_cache[cache_key] = mt
        return mt

    def _default_module_custom_fields(self):
        """Return custom_fields dict for new module creation."""
        return {
            "owner": self.default_owner,
            "record_completeness": "incomplete",
        }

    # ------------------------------------------------------------------ #
    #  Module Bay Management
    # ------------------------------------------------------------------ #

    def _ensure_module_bays(self, device, category, count):
        """
        Ensure the device has at least `count` module bays for `category`.
        Creates any missing bays.

        Returns:
            list of module bay records sorted by name
        """
        prefix = CATEGORIES[category]["prefix"]
        existing_bays = list(_api_retry(nb.dcim.module_bays.filter, device_id=device.id))
        category_bays = [b for b in existing_bays if b.name.startswith(f"{prefix}-")]
        existing_names = {b.name for b in category_bays}

        for i in range(count):
            bay_name = f"{prefix}-{i}"
            if bay_name not in existing_names:
                _api_retry(nb.dcim.module_bays.create, {
                    "device": device.id,
                    "name": bay_name,
                    "position": bay_name,
                })
                logger.info("Created module bay '%s' on device '%s'", bay_name, device.name)

        # Re-fetch to get the complete list
        all_bays = list(_api_retry(nb.dcim.module_bays.filter, device_id=device.id))
        category_bays = sorted(
            [b for b in all_bays if b.name.startswith(f"{prefix}-")],
            key=lambda b: b.name,
        )
        return category_bays

    def _get_device_modules(self, device, category):
        """
        Get all modules installed on a device in a given category.

        Returns:
            list of module records
        """
        prefix = CATEGORIES[category]["prefix"]
        all_modules = list(_api_retry(nb.dcim.modules.filter, device_id=device.id))
        # Filter by bay name prefix
        category_modules = []
        for mod in all_modules:
            bay = mod.module_bay
            if bay and hasattr(bay, "name") and bay.name.startswith(f"{prefix}-"):
                category_modules.append(mod)
            elif bay and hasattr(bay, "display") and bay.display.startswith(f"{prefix}-"):
                category_modules.append(mod)
        return category_modules

    # ------------------------------------------------------------------ #
    #  Re-parenting Logic
    # ------------------------------------------------------------------ #

    def _get_spare_device(self):
        """Get (cached) the SPARE-INVENTORY device."""
        if self._spare_device is not None:
            return self._spare_device
        spare_name = getattr(self.config, "spare_device_name", "SPARE-INVENTORY")
        self._spare_device = _api_retry(nb.dcim.devices.get, name=spare_name)
        if not self._spare_device:
            logger.error("Spare device '%s' not found in NetBox", spare_name)
        return self._spare_device

    def _find_module_by_serial(self, serial):
        """Search all modules by serial number. Returns first match or None."""
        if not serial:
            return None
        results = list(_api_retry(nb.dcim.modules.filter, serial=serial))
        if len(results) > 1:
            logger.warning("Duplicate serial '%s' found on %d modules — using first match", serial, len(results))
        return results[0] if results else None

    def _reparent_module(self, module, target_device, target_bay):
        """Move a module to a different device and bay."""
        logger.info(
            "Re-parenting module '%s' (serial=%s) → device '%s' bay '%s'",
            module, module.serial, target_device.name, target_bay.name,
        )
        module.device = target_device.id
        module.module_bay = target_bay.id
        _api_retry(module.save)

    def _move_to_spare(self, module, category):
        """Move a module to the SPARE-INVENTORY device."""
        spare = self._get_spare_device()
        if not spare:
            logger.error("Cannot move module to spare — spare device not found")
            return False

        prefix = CATEGORIES[category]["prefix"]
        spare_bays = list(_api_retry(nb.dcim.module_bays.filter, device_id=spare.id))
        spare_category_bays = sorted(
            [b for b in spare_bays if b.name.startswith(f"{prefix}-")],
            key=lambda b: b.name,
        )

        # Find an unoccupied bay
        spare_modules = list(_api_retry(nb.dcim.modules.filter, device_id=spare.id))
        occupied_bay_ids = set()
        for m in spare_modules:
            if m.module_bay:
                bay_id = m.module_bay.id if hasattr(m.module_bay, "id") else m.module_bay
                occupied_bay_ids.add(bay_id)

        target_bay = None
        for bay in spare_category_bays:
            if bay.id not in occupied_bay_ids:
                target_bay = bay
                break

        if not target_bay:
            logger.error(
                "No free %s bay on spare device — admin must expand spare bays", prefix
            )
            return False

        self._reparent_module(module, spare, target_bay)
        return True

    def _vacate_bay(self, bay, category):
        """If a bay is occupied, move its occupant to spare."""
        modules_in_bay = list(_api_retry(nb.dcim.modules.filter, module_bay_id=bay.id))
        for mod in modules_in_bay:
            logger.info("Bay '%s' occupied by module (serial=%s) — moving to spare", bay.name, mod.serial)
            self._move_to_spare(mod, category)

    # ------------------------------------------------------------------ #
    #  Core Sync Algorithm
    # ------------------------------------------------------------------ #

    def _sync_category(self, category, local_items):
        """
        Sync a single hardware category.

        Algorithm:
        1. Ensure device has enough module bays
        2. For each local item:
           - Serial on this device, correct bay → no-op (update module type if changed)
           - Serial on this device, wrong bay → update bay
           - Serial on spare → re-parent here
           - Serial on other device → re-parent (hardware moved)
           - Serial not found → create new module
        3. Existing modules NOT in local detection → move to spare
        """
        if not local_items:
            # Move all existing modules in this category to spare
            existing = self._get_device_modules(self.device, category)
            for mod in existing:
                self._move_to_spare(mod, category)
            return

        prefix = CATEGORIES[category]["prefix"]

        # Step 1: Ensure enough bays
        bays = self._ensure_module_bays(self.device, category, len(local_items))

        # Step 2: Get existing modules on this device for this category
        existing_modules = self._get_device_modules(self.device, category)
        existing_by_serial = {}
        for mod in existing_modules:
            if mod.serial:
                existing_by_serial[mod.serial] = mod

        matched_module_ids = set()
        has_serial = any(item.get("serial") for item in local_items)

        for idx, item in enumerate(local_items):
            serial = item.get("serial")
            bay = bays[idx] if idx < len(bays) else None

            if not bay:
                logger.warning(
                    "No bay available at index %d for %s on %s",
                    idx, prefix, self.device.name,
                )
                continue

            module_type = self._resolve_module_type(category, item)

            if serial:
                # --- Serial-based matching ---
                # Check if already on this device
                if serial in existing_by_serial:
                    mod = existing_by_serial[serial]
                    matched_module_ids.add(mod.id)
                    updated = False

                    # Check bay assignment
                    mod_bay_name = None
                    if mod.module_bay:
                        mod_bay_name = getattr(mod.module_bay, "name", None) or getattr(mod.module_bay, "display", None)
                    if mod_bay_name != bay.name:
                        self._vacate_bay(bay, category)
                        mod.module_bay = bay.id
                        updated = True

                    # Check module type
                    mod_mt_id = None
                    if mod.module_type:
                        mod_mt_id = mod.module_type.id if hasattr(mod.module_type, "id") else mod.module_type
                    if mod_mt_id != module_type.id:
                        mod.module_type = module_type.id
                        updated = True

                    if updated:
                        _api_retry(mod.save)
                        logger.info("Updated module serial=%s on %s", serial, self.device.name)
                    continue

                # Check if exists anywhere else
                remote_mod = self._find_module_by_serial(serial)
                if remote_mod:
                    matched_module_ids.add(remote_mod.id)
                    self._vacate_bay(bay, category)
                    self._reparent_module(remote_mod, self.device, bay)

                    # Update module type if changed
                    mod_mt_id = None
                    if remote_mod.module_type:
                        mod_mt_id = remote_mod.module_type.id if hasattr(remote_mod.module_type, "id") else remote_mod.module_type
                    if mod_mt_id != module_type.id:
                        remote_mod.module_type = module_type.id
                        _api_retry(remote_mod.save)
                    continue

                # Not found anywhere — create new
                self._vacate_bay(bay, category)
                new_mod = _api_retry(nb.dcim.modules.create, {
                    "device": self.device.id,
                    "module_bay": bay.id,
                    "module_type": module_type.id,
                    "serial": serial,
                    "status": "active",
                    "custom_fields": self._default_module_custom_fields(),
                })
                matched_module_ids.add(new_mod.id)
                logger.info(
                    "Created module %s serial=%s on %s bay=%s",
                    item["product"], serial, self.device.name, bay.name,
                )

            else:
                # --- No serial (e.g., CPUs): positional matching ---
                # Match by bay index position
                modules_in_bay = list(_api_retry(nb.dcim.modules.filter, module_bay_id=bay.id))
                if modules_in_bay:
                    mod = modules_in_bay[0]
                    matched_module_ids.add(mod.id)
                    # Update module type if changed
                    mod_mt_id = None
                    if mod.module_type:
                        mod_mt_id = mod.module_type.id if hasattr(mod.module_type, "id") else mod.module_type
                    if mod_mt_id != module_type.id:
                        mod.module_type = module_type.id
                        _api_retry(mod.save)
                        logger.info("Updated module type at %s on %s", bay.name, self.device.name)
                else:
                    new_mod = _api_retry(nb.dcim.modules.create, {
                        "device": self.device.id,
                        "module_bay": bay.id,
                        "module_type": module_type.id,
                        "status": "active",
                        "custom_fields": self._default_module_custom_fields(),
                    })
                    matched_module_ids.add(new_mod.id)
                    logger.info(
                        "Created module %s (no serial) on %s bay=%s",
                        item["product"], self.device.name, bay.name,
                    )

        # Step 3: Move unmatched existing modules to spare
        if has_serial:
            for mod in existing_modules:
                if mod.id not in matched_module_ids:
                    logger.info(
                        "Module serial=%s no longer detected on %s — moving to spare",
                        mod.serial, self.device.name,
                    )
                    self._move_to_spare(mod, category)

    # ------------------------------------------------------------------ #
    #  Public Interface
    # ------------------------------------------------------------------ #

    def create_or_update(self):
        """
        Main entry point: detect local hardware and sync all categories to NetBox.
        Must be called after the device exists in NetBox.
        """
        self.device = self.server.get_netbox_server()
        if not self.device:
            logger.error("Device not found in NetBox — cannot sync modules")
            return False

        logger.info("Starting module sync for device '%s' (id=%d)", self.device.name, self.device.id)

        # Detect all local hardware
        detections = {
            "cpu": self._get_local_cpus(),
            "gpu": self._get_local_gpus(),
            "dimm": self._get_local_dimms(),
            "ssd": self._get_local_ssds(),
            "nic": self._get_local_nics(),
            "psu": self._get_local_psus(),
        }

        for category, items in detections.items():
            logger.info("Detected %d %s(s)", len(items), category)
            try:
                self._sync_category(category, items)
            except Exception as e:
                logger.error("Failed to sync %s: %s", category, e)
                # Continue with other categories

        logger.info("Module sync complete for device '%s'", self.device.name)
        return True

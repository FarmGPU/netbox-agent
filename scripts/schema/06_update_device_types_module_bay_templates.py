#!/usr/bin/env python3
"""
06 — Add ModuleBayTemplates to device types and backfill module bays on existing devices.

Naming convention:
  CPU-0, CPU-1 for CPU sockets
  GPU-0..GPU-7 for GPU slots
  DIMM-0..DIMM-N for memory slots
  SSD-0..SSD-N for storage bays
  NIC-0, NIC-1 for add-in NICs
  PSU-0, PSU-1 for power supplies

Idempotent: skips templates and bays that already exist.
"""

import re
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from nb_connection import get_api, logger

STANDARD_BAY_PATTERN = re.compile(r"^(CPU|GPU|DIMM|SSD|NIC|PSU)-\d+$")

# Default bay counts per device type category
# These are reasonable defaults; customize per device type as needed
DEFAULT_BAY_COUNTS = {
    "CPU": 2,
    "GPU": 8,
    "DIMM": 16,
    "SSD": 8,
    "NIC": 2,
    "PSU": 2,
}

# Override bay counts for specific device types (by model keyword)
DEVICE_TYPE_OVERRIDES = {
    # GPU-heavy systems
    "DGX": {"GPU": 8, "CPU": 2, "DIMM": 32, "SSD": 8, "NIC": 4, "PSU": 2},
    "HGX": {"GPU": 8, "CPU": 2, "DIMM": 32, "SSD": 8, "NIC": 4, "PSU": 2},
    # Standard 1U servers
    "1U": {"GPU": 0, "CPU": 2, "DIMM": 16, "SSD": 4, "NIC": 2, "PSU": 2},
    # Standard 2U servers
    "2U": {"GPU": 2, "CPU": 2, "DIMM": 24, "SSD": 12, "NIC": 2, "PSU": 2},
}


def _get_bay_counts_for_type(device_type_model):
    """Determine bay counts based on device type model name."""
    model_upper = device_type_model.upper() if device_type_model else ""
    for keyword, counts in DEVICE_TYPE_OVERRIDES.items():
        if keyword.upper() in model_upper:
            return counts
    return DEFAULT_BAY_COUNTS.copy()


def _ensure_module_bay_templates(nb, device_type):
    """Add standard templates and remove legacy-named ones from a device type."""
    bay_counts = _get_bay_counts_for_type(device_type.model)
    existing_templates = list(nb.dcim.module_bay_templates.filter(device_type_id=device_type.id))
    existing_names = {t.name for t in existing_templates}

    # Remove legacy templates (names not matching CATEGORY-N convention)
    deleted = 0
    for t in existing_templates:
        if not STANDARD_BAY_PATTERN.match(t.name):
            logger.info(
                "  Deleting legacy template '%s' from device type '%s'",
                t.name, device_type.model,
            )
            t.delete()
            deleted += 1

    # Add standard templates
    created = 0
    for category, count in bay_counts.items():
        for i in range(count):
            bay_name = f"{category}-{i}"
            if bay_name in existing_names:
                continue
            nb.dcim.module_bay_templates.create({
                "device_type": device_type.id,
                "name": bay_name,
                "position": bay_name,
            })
            created += 1

    if created > 0 or deleted > 0:
        logger.info(
            "Device type '%s': added %d, deleted %d legacy template(s)",
            device_type.model, created, deleted,
        )
    else:
        logger.info("Device type '%s' — all bay templates present", device_type.model)


def _backfill_device_module_bays(nb, device):
    """Create standard bays and remove empty legacy bays on an existing device."""
    templates = list(nb.dcim.module_bay_templates.filter(device_type_id=device.device_type.id))

    existing_bays = list(nb.dcim.module_bays.filter(device_id=device.id))
    existing_names = {b.name for b in existing_bays}

    # Remove empty legacy bays
    deleted = 0
    for bay in existing_bays:
        if not STANDARD_BAY_PATTERN.match(bay.name) and not bay.installed_module:
            logger.info("  Deleting legacy bay '%s' on %s", bay.name, device.name)
            bay.delete()
            deleted += 1

    # Add missing standard bays from templates
    created = 0
    if templates:
        for template in templates:
            if template.name in existing_names:
                continue
            nb.dcim.module_bays.create({
                "device": device.id,
                "name": template.name,
                "position": template.name,
            })
            created += 1

    return created + deleted  # Return total changes for logging


def run(nb):
    # Step 1: Add module bay templates to all device types
    device_types = list(nb.dcim.device_types.all())
    logger.info("Processing %d device type(s) for module bay templates...", len(device_types))

    for dt in device_types:
        _ensure_module_bay_templates(nb, dt)

    # Step 2: Backfill module bays on all existing devices
    devices = list(nb.dcim.devices.all())
    logger.info("Backfilling module bays on %d existing device(s)...", len(devices))

    total_changes = 0
    for device in devices:
        changes = _backfill_device_module_bays(nb, device)
        if changes > 0:
            logger.info("  %s: %d change(s)", device.name, changes)
            total_changes += changes

    logger.info("Backfill complete: %d total change(s)", total_changes)


def main():
    nb = get_api()
    run(nb)


if __name__ == "__main__":
    main()

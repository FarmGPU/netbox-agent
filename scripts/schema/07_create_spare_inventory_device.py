#!/usr/bin/env python3
"""
07 — Create the SPARE-INVENTORY virtual device.

Creates:
  - "FarmGPU" manufacturer (if needed)
  - "Virtual Spare Pool" device type with generous module bay counts
  - "Spare Pool" device role
  - "SPARE-INVENTORY" device at SMF1 site

Idempotent: skips objects that already exist.
"""

import re
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from nb_connection import get_api, logger

MANUFACTURER_NAME = "FarmGPU"
DEVICE_TYPE_MODEL = "Virtual Spare Pool"
DEVICE_ROLE_NAME = "Spare Pool"
DEVICE_NAME = "SPARE-INVENTORY"
SITE_SLUG = "smf01"

SPARE_BAY_COUNTS = {
    "GPU": 200,
    "CPU": 100,
    "DIMM": 500,
    "SSD": 200,
    "NIC": 100,
    "PSU": 100,
}


def _get_or_create_manufacturer(nb, name):
    mfr = nb.dcim.manufacturers.get(name=name)
    if not mfr:
        slug = re.sub(r"[^A-Za-z0-9]+", "-", name).lower().strip("-")
        mfr = nb.dcim.manufacturers.get(slug=slug)
        if mfr:
            logger.info("Manufacturer '%s' found by slug '%s' (actual name='%s')", name, slug, mfr.name)
        else:
            mfr = nb.dcim.manufacturers.create(name=name, slug=slug)
            logger.info("Created manufacturer '%s'", name)
    else:
        logger.info("Manufacturer '%s' already exists", name)
    return mfr


def _get_or_create_device_type(nb, manufacturer, model):
    dt = nb.dcim.device_types.get(manufacturer_id=manufacturer.id, model=model)
    if not dt:
        slug = re.sub(r"[^A-Za-z0-9]+", "-", model).lower().strip("-")
        dt = nb.dcim.device_types.create({
            "manufacturer": manufacturer.id,
            "model": model,
            "slug": slug,
        })
        logger.info("Created device type '%s'", model)
    else:
        logger.info("Device type '%s' already exists", model)

    # Ensure module bay templates exist
    existing_templates = list(nb.dcim.module_bay_templates.filter(device_type_id=dt.id))
    existing_names = {t.name for t in existing_templates}
    created = 0
    for category, count in SPARE_BAY_COUNTS.items():
        for i in range(count):
            bay_name = f"{category}-{i}"
            if bay_name not in existing_names:
                nb.dcim.module_bay_templates.create({
                    "device_type": dt.id,
                    "name": bay_name,
                    "position": bay_name,
                })
                created += 1
    if created > 0:
        logger.info("Created %d module bay templates on '%s'", created, model)

    return dt


def _get_or_create_device_role(nb, name):
    slug = re.sub(r"[^A-Za-z0-9]+", "-", name).lower().strip("-")
    role = nb.dcim.device_roles.get(slug=slug)
    if not role:
        role = nb.dcim.device_roles.create({
            "name": name,
            "slug": slug,
            "vm_role": False,
        })
        logger.info("Created device role '%s'", name)
    else:
        logger.info("Device role '%s' already exists", name)
    return role


def run(nb):
    # Get site
    site = nb.dcim.sites.get(slug=SITE_SLUG)
    if not site:
        logger.error("Site '%s' not found — create it manually first", SITE_SLUG)
        sys.exit(1)

    manufacturer = _get_or_create_manufacturer(nb, MANUFACTURER_NAME)
    device_type = _get_or_create_device_type(nb, manufacturer, DEVICE_TYPE_MODEL)
    device_role = _get_or_create_device_role(nb, DEVICE_ROLE_NAME)

    # Create the SPARE-INVENTORY device
    existing = nb.dcim.devices.get(name=DEVICE_NAME)
    if existing:
        logger.info("Device '%s' already exists (id=%d)", DEVICE_NAME, existing.id)
    else:
        device = nb.dcim.devices.create({
            "name": DEVICE_NAME,
            "device_type": device_type.id,
            "role": device_role.id,
            "site": site.id,
            "status": "inventory",
            "custom_fields": {
                "owner": "FarmGPU",
            },
        })
        logger.info("Created device '%s' (id=%d)", DEVICE_NAME, device.id)

        # Backfill module bays on the device
        templates = list(nb.dcim.module_bay_templates.filter(device_type_id=device_type.id))
        created = 0
        for t in templates:
            nb.dcim.module_bays.create({
                "device": device.id,
                "name": t.name,
                "position": t.name,
            })
            created += 1
        logger.info("Created %d module bays on '%s'", created, DEVICE_NAME)


def main():
    nb = get_api()
    run(nb)


if __name__ == "__main__":
    main()

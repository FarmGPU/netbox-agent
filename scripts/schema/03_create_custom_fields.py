#!/usr/bin/env python3
"""
03 — Create custom fields and update bmc_mac_address.

Creates: owner, environment, chassis_serial, record_completeness, reservation_end.
Updates: bmc_mac_address (add regex validation, set required=False).
Idempotent: skips fields that already exist with correct configuration.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from nb_connection import get_api, logger


def _get_choice_set_id(nb, name):
    cs = nb.extras.custom_field_choice_sets.get(name=name)
    if not cs:
        raise RuntimeError(f"Choice set '{name}' not found — run 02 first")
    return cs.id


def run(nb):
    owner_cs = _get_choice_set_id(nb, "OwnerChoices")
    env_cs = _get_choice_set_id(nb, "EnvironmentChoices")
    completeness_cs = _get_choice_set_id(nb, "RecordCompletenessChoices")

    # NetBox 4.x accepts object_types as strings directly (e.g., "dcim.device")
    fields = [
        {
            "name": "owner",
            "label": "Owner",
            "type": "select",
            "object_types": ["dcim.device", "dcim.module"],
            "choice_set": owner_cs,
            "required": True,
            "description": "Hardware owner organization",
            "weight": 100,
        },
        {
            "name": "environment",
            "label": "Environment",
            "type": "select",
            "object_types": ["dcim.device"],
            "choice_set": env_cs,
            "required": False,
            "default": "Production",
            "description": "Deployment environment",
            "weight": 200,
        },
        {
            "name": "chassis_serial",
            "label": "Chassis Serial",
            "type": "text",
            "object_types": ["dcim.device"],
            "required": False,
            "description": "Secondary chassis serial for dual-serial tracking",
            "weight": 300,
        },
        {
            "name": "record_completeness",
            "label": "Record Completeness",
            "type": "select",
            "object_types": ["dcim.device", "dcim.module"],
            "choice_set": completeness_cs,
            "required": False,
            "default": "incomplete",
            "description": "Whether this record meets field-completeness standards",
            "weight": 400,
        },
        {
            "name": "reservation_end",
            "label": "Reservation End",
            "type": "date",
            "object_types": ["dcim.device"],
            "required": False,
            "description": "Lease or reservation expiry date",
            "weight": 500,
        },
        {
            "name": "missing_agent_dependencies",
            "label": "Missing Agent Dependencies",
            "type": "text",
            "object_types": ["dcim.device"],
            "required": False,
            "description": "Comma-separated list of tools missing on this server (auto-populated by netbox-agent)",
            "weight": 700,
        },
    ]

    for field_def in fields:
        existing = nb.extras.custom_fields.get(name=field_def["name"])
        if existing:
            logger.info("Custom field '%s' already exists (id=%d) — skipping", field_def["name"], existing.id)
            continue

        result = nb.extras.custom_fields.create(field_def)
        logger.info("Created custom field '%s' (id=%d)", field_def["name"], result.id)

    # Update bmc_mac_address: ensure required=False, unique=False
    # (validation_regex is already set from the original creation)
    bmc = nb.extras.custom_fields.get(name="bmc_mac_address")
    if bmc:
        update_needed = False
        if getattr(bmc, "required", None) is True:
            bmc.required = False
            update_needed = True
        if getattr(bmc, "unique", None) is True:
            bmc.unique = False
            update_needed = True
        if update_needed:
            bmc.save()
            logger.info("Updated bmc_mac_address: required=False, unique=False")
        else:
            logger.info("bmc_mac_address already configured correctly — skipping")
    else:
        logger.warning("bmc_mac_address custom field not found — creating it")
        nb.extras.custom_fields.create({
            "name": "bmc_mac_address",
            "label": "BMC MAC Address",
            "type": "text",
            "object_types": ["dcim.device"],
            "required": False,
            "validation_regex": r"^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$",
            "description": "BMC/IPMI MAC address",
            "weight": 600,
        })
        logger.info("Created bmc_mac_address custom field")


def main():
    nb = get_api()
    run(nb)


if __name__ == "__main__":
    main()

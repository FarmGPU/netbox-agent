#!/usr/bin/env python3
"""
Validate record completeness for all devices and modules.

Checks every device and module against the Record Standards field matrix,
updates cf_record_completeness to "complete" or "incomplete", and outputs
a fleet-wide completeness summary.

Device required: name, serial, device_type, site, rack, role, status, cf_owner, cf_environment
Device preferred: asset_tag
Module required: serial, module_type, module_bay, status, cf_owner

Usage:
    NETBOX_URL=... NETBOX_TOKEN=... python validate_record_completeness.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "schema"))
from nb_connection import get_api, logger


def _check_device_completeness(device):
    """Check if a device meets the required field standard. Returns (is_complete, missing_fields)."""
    missing = []

    if not device.name:
        missing.append("name")
    if not device.serial:
        missing.append("serial")
    if not device.device_type:
        missing.append("device_type")
    if not device.site:
        missing.append("site")
    if not device.rack:
        missing.append("rack")
    if not device.role:
        missing.append("role")
    if not device.status:
        missing.append("status")

    cf = device.custom_fields or {}
    if not cf.get("owner"):
        missing.append("cf_owner")
    if not cf.get("environment"):
        missing.append("cf_environment")

    return len(missing) == 0, missing


def _check_device_preferred(device):
    """Check preferred (non-required) fields. Returns list of missing preferred fields."""
    missing = []
    if not device.asset_tag:
        missing.append("asset_tag")
    return missing


def _check_module_completeness(module):
    """Check if a module meets the required field standard. Returns (is_complete, missing_fields)."""
    missing = []

    if not module.serial:
        missing.append("serial")
    if not module.module_type:
        missing.append("module_type")
    if not module.module_bay:
        missing.append("module_bay")
    if not module.status:
        missing.append("status")

    cf = module.custom_fields or {}
    if not cf.get("owner"):
        missing.append("cf_owner")

    return len(missing) == 0, missing


def run(nb):
    devices = list(nb.dcim.devices.all())
    modules = list(nb.dcim.modules.all())

    logger.info("Validating %d device(s) and %d module(s)...", len(devices), len(modules))

    # Device validation
    device_complete = 0
    device_incomplete = 0
    device_issues = []

    for device in devices:
        is_complete, missing = _check_device_completeness(device)
        preferred_missing = _check_device_preferred(device)
        new_status = "complete" if is_complete else "incomplete"

        cf = device.custom_fields or {}
        if cf.get("record_completeness") != new_status:
            device.custom_fields = {**cf, "record_completeness": new_status}
            try:
                device.save()
            except Exception as e:
                logger.error("Failed to update device %s: %s", device.name, e)

        if is_complete:
            device_complete += 1
        else:
            device_incomplete += 1
            device_issues.append({
                "name": device.name,
                "missing_required": missing,
                "missing_preferred": preferred_missing,
            })

    # Module validation
    module_complete = 0
    module_incomplete = 0
    module_issues = []

    for module in modules:
        is_complete, missing = _check_module_completeness(module)
        new_status = "complete" if is_complete else "incomplete"

        cf = module.custom_fields or {}
        if cf.get("record_completeness") != new_status:
            module.custom_fields = {**cf, "record_completeness": new_status}
            try:
                module.save()
            except Exception as e:
                logger.error("Failed to update module %s: %s", module.id, e)

        if is_complete:
            module_complete += 1
        else:
            module_incomplete += 1
            module_issues.append({
                "id": module.id,
                "serial": module.serial,
                "missing_required": missing,
            })

    # Print summary
    total_devices = len(devices)
    total_modules = len(modules)

    print("\n" + "=" * 70)
    print("RECORD COMPLETENESS VALIDATION REPORT")
    print("=" * 70)

    print(f"\nDEVICES: {total_devices} total")
    if total_devices > 0:
        pct = (device_complete / total_devices) * 100
        print(f"  Complete:   {device_complete} ({pct:.1f}%)")
        print(f"  Incomplete: {device_incomplete} ({100 - pct:.1f}%)")

    print(f"\nMODULES: {total_modules} total")
    if total_modules > 0:
        pct = (module_complete / total_modules) * 100
        print(f"  Complete:   {module_complete} ({pct:.1f}%)")
        print(f"  Incomplete: {module_incomplete} ({100 - pct:.1f}%)")

    if device_issues:
        print(f"\nINCOMPLETE DEVICES ({len(device_issues)}):")
        print("-" * 70)
        for issue in device_issues[:50]:  # Cap output at 50
            print(f"  {issue['name']}: missing {issue['missing_required']}")
            if issue["missing_preferred"]:
                print(f"    preferred missing: {issue['missing_preferred']}")
        if len(device_issues) > 50:
            print(f"  ... and {len(device_issues) - 50} more")

    if module_issues:
        print(f"\nINCOMPLETE MODULES ({len(module_issues)}):")
        print("-" * 70)
        for issue in module_issues[:50]:
            print(f"  Module {issue['id']} (serial={issue['serial']}): missing {issue['missing_required']}")
        if len(module_issues) > 50:
            print(f"  ... and {len(module_issues) - 50} more")

    print()
    return device_incomplete == 0 and module_incomplete == 0


def main():
    nb = get_api()
    all_complete = run(nb)
    sys.exit(0 if all_complete else 1)


if __name__ == "__main__":
    main()

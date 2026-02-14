#!/usr/bin/env python3
"""
Verification script: compare module data vs inventory item data per device.

Compares inventory item counts/serials against module counts/serials
per device per category. Outputs a discrepancy report.

Usage:
    NETBOX_URL=... NETBOX_TOKEN=... python verify_modules_vs_inventory.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "schema"))
from nb_connection import get_api, logger

# Map inventory item tags to module bay prefixes
TAG_TO_CATEGORY = {
    "hw-cpu": "CPU",
    "hw-gpu": "GPU",
    "hw-memory": "DIMM",
    "hw-disk": "SSD",
    "hw-interface": "NIC",
}


def _get_inventory_items_by_device(nb, device_id):
    """Get inventory items grouped by category tag."""
    result = {}
    for tag_slug, category in TAG_TO_CATEGORY.items():
        items = list(nb.dcim.inventory_items.filter(device_id=device_id, tag=tag_slug))
        result[category] = {
            "count": len(items),
            "serials": sorted(set(i.serial for i in items if i.serial)),
        }
    return result


def _get_modules_by_device(nb, device_id):
    """Get modules grouped by bay prefix category."""
    all_modules = list(nb.dcim.modules.filter(device_id=device_id))
    result = {}
    for category in TAG_TO_CATEGORY.values():
        result[category] = {"count": 0, "serials": []}

    for mod in all_modules:
        bay_name = ""
        if mod.module_bay:
            bay_name = getattr(mod.module_bay, "name", "") or getattr(mod.module_bay, "display", "")
        for category in TAG_TO_CATEGORY.values():
            if bay_name.startswith(f"{category}-"):
                result[category]["count"] += 1
                if mod.serial:
                    result[category]["serials"].append(mod.serial)
                break

    for category in result:
        result[category]["serials"] = sorted(set(result[category]["serials"]))

    return result


def run(nb):
    devices = list(nb.dcim.devices.all())
    logger.info("Checking %d device(s)...", len(devices))

    discrepancies = []
    match_count = 0
    total_checks = 0

    for device in devices:
        if device.name == "SPARE-INVENTORY":
            continue

        inv = _get_inventory_items_by_device(nb, device.id)
        mod = _get_modules_by_device(nb, device.id)

        for category in TAG_TO_CATEGORY.values():
            total_checks += 1
            inv_data = inv.get(category, {"count": 0, "serials": []})
            mod_data = mod.get(category, {"count": 0, "serials": []})

            count_match = inv_data["count"] == mod_data["count"]
            # For serials, check that module serials are a superset of inventory serials
            inv_serials = set(inv_data["serials"])
            mod_serials = set(mod_data["serials"])
            serial_match = inv_serials == mod_serials or (not inv_serials and not mod_serials)

            if count_match and serial_match:
                match_count += 1
            else:
                d = {
                    "device": device.name,
                    "category": category,
                    "inv_count": inv_data["count"],
                    "mod_count": mod_data["count"],
                    "inv_only_serials": sorted(inv_serials - mod_serials),
                    "mod_only_serials": sorted(mod_serials - inv_serials),
                }
                discrepancies.append(d)

    # Print report
    print("\n" + "=" * 70)
    print("MODULES vs INVENTORY ITEMS — VERIFICATION REPORT")
    print("=" * 70)
    print(f"Devices checked: {len(devices)}")
    print(f"Category checks: {total_checks}")
    print(f"Matches: {match_count}")
    print(f"Discrepancies: {len(discrepancies)}")
    print()

    if discrepancies:
        print("DISCREPANCIES:")
        print("-" * 70)
        for d in discrepancies:
            print(f"  Device: {d['device']}")
            print(f"    Category: {d['category']}")
            print(f"    Inventory items: {d['inv_count']}, Modules: {d['mod_count']}")
            if d["inv_only_serials"]:
                print(f"    In inventory only: {d['inv_only_serials']}")
            if d["mod_only_serials"]:
                print(f"    In modules only: {d['mod_only_serials']}")
            print()
    else:
        print("All inventory items match modules. Migration verified!")

    return len(discrepancies) == 0


def main():
    nb = get_api()
    success = run(nb)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

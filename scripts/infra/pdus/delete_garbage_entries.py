#!/usr/bin/env python3
"""
Step 1: Delete 3 garbage PDU device entries from NetBox.

Targets (verified by human review):
  ID 202  |PDU19-7-14      Duplicate with pipe char prefix
  ID 232  smf010301-pdu01  Empty OEM placeholder
  ID 233  smf010301-pdu02  Empty OEM placeholder

Usage:
    NETBOX_URL=... NETBOX_TOKEN=... python3 delete_garbage_entries.py --dry-run
    NETBOX_URL=... NETBOX_TOKEN=... python3 delete_garbage_entries.py
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "schema"))
from nb_connection import get_api, logger

# Hard-coded targets — each tuple: (device_id, expected_name, reason)
GARBAGE_DEVICES = [
    (202, "|PDU19-7-14",     "Duplicate with pipe char prefix"),
    (232, "smf010301-pdu01", "Empty OEM placeholder"),
    (233, "smf010301-pdu02", "Empty OEM placeholder"),
]


def main():
    parser = argparse.ArgumentParser(description="Delete garbage PDU entries from NetBox")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted")
    args = parser.parse_args()
    dry = args.dry_run

    nb = get_api()

    if dry:
        print("=== DRY RUN — no changes will be made ===\n")

    print(f"Targets: {len(GARBAGE_DEVICES)} devices to delete\n")

    errors = []
    for device_id, expected_name, reason in GARBAGE_DEVICES:
        device = nb.dcim.devices.get(device_id)
        if device is None:
            msg = f"  ID {device_id}: NOT FOUND (already deleted?)"
            print(msg)
            errors.append(msg)
            continue

        if device.name != expected_name:
            msg = (f"  ID {device_id}: NAME MISMATCH — expected '{expected_name}', "
                   f"got '{device.name}'. SKIPPING for safety.")
            print(msg)
            errors.append(msg)
            continue

        serial = device.serial or "(none)"
        dtype = device.device_type.model if device.device_type else "(none)"
        print(f"  ID {device_id}: name='{device.name}' serial={serial} "
              f"type={dtype} reason={reason}")

    if errors:
        print(f"\n{len(errors)} problem(s) found. Review above before proceeding.")

    if dry:
        print("\n=== DRY RUN complete — re-run without --dry-run to execute ===")
        return

    # Prompt for confirmation
    print()
    confirm = input("Type YES to confirm deletion: ")
    if confirm != "YES":
        print("Aborted.")
        return

    deleted = 0
    for device_id, expected_name, reason in GARBAGE_DEVICES:
        device = nb.dcim.devices.get(device_id)
        if device is None or device.name != expected_name:
            continue
        try:
            device.delete()
            logger.info("Deleted device id=%d name='%s'", device_id, expected_name)
            deleted += 1
        except Exception as e:
            logger.error("Failed to delete id=%d name='%s': %s", device_id, expected_name, e)

    print(f"\nDeleted {deleted}/{len(GARBAGE_DEVICES)} devices.")


if __name__ == "__main__":
    main()

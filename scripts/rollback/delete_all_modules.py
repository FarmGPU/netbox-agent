#!/usr/bin/env python3
"""
Rollback script: delete all modules from NetBox.

Level 2 rollback — removes all module objects while inventory items remain intact.

Usage:
    NETBOX_URL=... NETBOX_TOKEN=... python delete_all_modules.py --dry-run
    NETBOX_URL=... NETBOX_TOKEN=... python delete_all_modules.py
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "schema"))
from nb_connection import get_api, logger


def run(nb, dry_run=True):
    modules = list(nb.dcim.modules.all())

    print(f"\nTotal modules in NetBox: {len(modules)}")

    if not modules:
        print("No modules to delete.")
        return

    # Group by device for summary
    by_device = {}
    for mod in modules:
        device_name = "Unknown"
        if mod.device:
            device_name = getattr(mod.device, "name", None) or getattr(mod.device, "display", str(mod.device))
        by_device.setdefault(device_name, []).append(mod)

    print("\nModules by device:")
    for device_name in sorted(by_device):
        print(f"  {device_name}: {len(by_device[device_name])}")

    if dry_run:
        print("\n[DRY RUN] No modules were deleted. Run without --dry-run to delete.")
        return

    # Require confirmation
    print("\nThis will permanently delete ALL modules from NetBox.")
    confirm = input("Type YES to confirm: ")
    if confirm != "YES":
        print("Aborted.")
        return

    deleted = 0
    for mod in modules:
        try:
            mod.delete()
            deleted += 1
            if deleted % 50 == 0:
                logger.info("Deleted %d / %d modules...", deleted, len(modules))
        except Exception as e:
            logger.error("Failed to delete module %d: %s", mod.id, e)

    logger.info("Rollback complete: %d / %d modules deleted", deleted, len(modules))


def main():
    parser = argparse.ArgumentParser(description="Delete all modules (rollback)")
    parser.add_argument("--dry-run", action="store_true", help="Preview what would be deleted")
    args = parser.parse_args()

    nb = get_api()
    run(nb, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

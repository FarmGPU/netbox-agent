#!/usr/bin/env python3
"""
Bulk delete old hw:*-tagged inventory items.

Usage:
    NETBOX_URL=... NETBOX_TOKEN=... python delete_old_inventory_items.py --dry-run
    NETBOX_URL=... NETBOX_TOKEN=... python delete_old_inventory_items.py

Requires typing YES to confirm in non-dry-run mode.
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "schema"))
from nb_connection import get_api, logger

HW_TAGS = [
    "hw-cpu",
    "hw-gpu",
    "hw-disk",
    "hw-interface",
    "hw-memory",
    "hw-motherboard",
    "hw-raid-card",
]


def run(nb, dry_run=True):
    total = 0
    items_by_tag = {}

    for tag_slug in HW_TAGS:
        items = list(nb.dcim.inventory_items.filter(tag=tag_slug))
        items_by_tag[tag_slug] = items
        total += len(items)

    print(f"\nInventory items to delete: {total}")
    for tag_slug, items in items_by_tag.items():
        print(f"  {tag_slug}: {len(items)}")

    if total == 0:
        print("Nothing to delete.")
        return

    if dry_run:
        print("\n[DRY RUN] No items were deleted. Run without --dry-run to delete.")
        return

    # Require confirmation
    print("\nThis will permanently delete all hw:*-tagged inventory items.")
    confirm = input("Type YES to confirm: ")
    if confirm != "YES":
        print("Aborted.")
        return

    deleted = 0
    for tag_slug, items in items_by_tag.items():
        for item in items:
            try:
                item.delete()
                deleted += 1
                if deleted % 100 == 0:
                    logger.info("Deleted %d / %d items...", deleted, total)
            except Exception as e:
                logger.error("Failed to delete item %d (%s): %s", item.id, item.name, e)

    logger.info("Deletion complete: %d / %d items deleted", deleted, total)


def main():
    parser = argparse.ArgumentParser(description="Delete old hw:* inventory items")
    parser.add_argument("--dry-run", action="store_true", help="Preview what would be deleted")
    args = parser.parse_args()

    nb = get_api()
    run(nb, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

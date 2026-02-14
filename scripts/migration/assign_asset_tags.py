#!/usr/bin/env python3
"""
Assign sequential Base-36 asset tags to devices without one.

Tags are 4-character Base-36 strings: 0000, 0001, ..., 000Z, 0010, ...
Maximum: ZZZZ = 1,679,615 unique tags.

Usage:
    NETBOX_URL=... NETBOX_TOKEN=... python assign_asset_tags.py --dry-run
    NETBOX_URL=... NETBOX_TOKEN=... python assign_asset_tags.py
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "schema"))
from nb_connection import get_api, logger

BASE36_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def int_to_base36(n, width=4):
    """Convert integer to zero-padded Base-36 string."""
    if n < 0:
        raise ValueError("Negative numbers not supported")
    if n == 0:
        return "0" * width
    digits = []
    while n:
        digits.append(BASE36_CHARS[n % 36])
        n //= 36
    result = "".join(reversed(digits))
    return result.zfill(width)


def base36_to_int(s):
    """Convert Base-36 string to integer."""
    return int(s, 36)


def run(nb, dry_run=True):
    devices = list(nb.dcim.devices.all())

    # Find the highest existing asset tag to continue sequencing
    max_tag_val = -1
    tagged = 0
    untagged = []

    for device in devices:
        if device.asset_tag:
            tagged += 1
            try:
                val = base36_to_int(device.asset_tag)
                if val > max_tag_val:
                    max_tag_val = val
            except ValueError:
                pass  # Non-base36 tag, skip
        else:
            untagged.append(device)

    next_val = max_tag_val + 1

    print(f"\nDevices total: {len(devices)}")
    print(f"Already tagged: {tagged}")
    print(f"Need tags: {len(untagged)}")
    print(f"Starting tag: {int_to_base36(next_val)}")
    print(f"Ending tag: {int_to_base36(next_val + len(untagged) - 1) if untagged else 'N/A'}")

    if not untagged:
        print("All devices already have asset tags.")
        return

    if dry_run:
        print("\n[DRY RUN] Assignments that would be made:")
        for i, device in enumerate(sorted(untagged, key=lambda d: d.name or "")):
            tag = int_to_base36(next_val + i)
            print(f"  {device.name}: {tag}")
        print("\nRun without --dry-run to apply.")
        return

    # Apply tags
    assigned = 0
    for i, device in enumerate(sorted(untagged, key=lambda d: d.name or "")):
        tag = int_to_base36(next_val + i)
        try:
            device.asset_tag = tag
            device.save()
            assigned += 1
            logger.info("Assigned asset_tag %s to %s", tag, device.name)
        except Exception as e:
            logger.error("Failed to assign tag to %s: %s", device.name, e)

    logger.info("Assignment complete: %d / %d devices tagged", assigned, len(untagged))


def main():
    parser = argparse.ArgumentParser(description="Assign Base-36 asset tags to devices")
    parser.add_argument("--dry-run", action="store_true", help="Preview assignments without applying")
    args = parser.parse_args()

    nb = get_api()
    run(nb, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

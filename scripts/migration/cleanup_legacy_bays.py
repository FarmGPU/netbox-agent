#!/usr/bin/env python3
"""
One-time cleanup: delete legacy module bay templates and module bays that
don't follow our standard naming convention (CATEGORY-N, e.g. PSU-0).

Standard pattern: CPU-N, GPU-N, DIMM-N, SSD-N, NIC-N, PSU-N
Legacy examples:  PSU1, PSU2, DIMM1, PS1, "AIOM 1", "M.2 Slot 1", "PCIe Slot 2"

Safety:
  - Only deletes EMPTY bays (no installed module)
  - Warns about occupied legacy bays (requires manual intervention)
  - Templates are always safe to delete (they're blueprints, not data)

Usage:
    NETBOX_URL=https://localhost NETBOX_TOKEN=<token> python3 cleanup_legacy_bays.py --dry-run
    NETBOX_URL=https://localhost NETBOX_TOKEN=<token> python3 cleanup_legacy_bays.py
"""

import argparse
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "schema"))
from nb_connection import get_api

STANDARD_BAY_PATTERN = re.compile(r"^(CPU|GPU|DIMM|SSD|NIC|PSU)-\d+$")


def _paginate_all(endpoint, **filters):
    """Fetch all records from a paginated NetBox endpoint."""
    results = []
    limit = 1000
    offset = 0
    while True:
        page = list(endpoint.filter(limit=limit, offset=offset, **filters))
        if not page:
            break
        results.extend(page)
        if len(page) < limit:
            break
        offset += limit
    return results


def cleanup_templates(nb, dry):
    """Delete module bay templates that don't match standard naming."""
    all_templates = _paginate_all(nb.dcim.module_bay_templates)
    print(f"Scanning {len(all_templates)} module bay template(s)...")

    legacy = [t for t in all_templates if not STANDARD_BAY_PATTERN.match(t.name)]
    if not legacy:
        print("  No legacy templates found.\n")
        return 0

    # Group by device type for readable output
    by_dt = {}
    for t in legacy:
        dt_display = t.device_type.display if t.device_type else "Unknown"
        dt_id = t.device_type.id if t.device_type else "?"
        key = dt_id
        if key not in by_dt:
            by_dt[key] = {"display": dt_display, "templates": []}
        by_dt[key]["templates"].append(t)

    deleted = 0
    for dt_id, info in sorted(by_dt.items()):
        names = [t.name for t in info["templates"]]
        print(f"  Device type {dt_id} ({info['display']}): {names}")
        for t in info["templates"]:
            print(f"    DELETE template id={t.id} name={t.name}")
            if not dry:
                t.delete()
            deleted += 1

    print(f"\n  Templates deleted: {deleted}\n")
    return deleted


def cleanup_bays(nb, dry):
    """Delete empty module bays that don't match standard naming."""
    all_bays = _paginate_all(nb.dcim.module_bays)
    print(f"Scanning {len(all_bays)} module bay(s)...")

    legacy = [b for b in all_bays if not STANDARD_BAY_PATTERN.match(b.name)]
    if not legacy:
        print("  No legacy bays found.\n")
        return 0

    # Separate empty vs occupied
    empty_legacy = [b for b in legacy if not b.installed_module]
    occupied_legacy = [b for b in legacy if b.installed_module]

    if occupied_legacy:
        print(f"\n  WARNING: {len(occupied_legacy)} occupied legacy bay(s) — SKIPPED:")
        for b in occupied_legacy:
            dev_name = b.device.display if b.device else "?"
            print(f"    bay id={b.id} name={b.name} device={dev_name} — has installed module")
        print("  These require manual intervention (move module first).\n")

    # Group empty bays by device
    by_dev = {}
    for b in empty_legacy:
        dev_name = b.device.display if b.device else "Unknown"
        dev_id = b.device.id if b.device else "?"
        key = dev_id
        if key not in by_dev:
            by_dev[key] = {"display": dev_name, "bays": []}
        by_dev[key]["bays"].append(b)

    deleted = 0
    for dev_id, info in sorted(by_dev.items()):
        names = [b.name for b in info["bays"]]
        print(f"  Device {dev_id} ({info['display']}): {names}")
        for b in info["bays"]:
            print(f"    DELETE bay id={b.id} name={b.name}")
            if not dry:
                b.delete()
            deleted += 1

    print(f"\n  Bays deleted: {deleted}\n")
    return deleted


def main():
    parser = argparse.ArgumentParser(
        description="Clean up legacy module bay templates and bays"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print what would be deleted"
    )
    args = parser.parse_args()
    dry = args.dry_run

    nb = get_api()

    if dry:
        print("=== DRY RUN --- no changes will be made ===\n")

    print("--- Phase 1: Module Bay Templates ---")
    tpl_count = cleanup_templates(nb, dry)

    print("--- Phase 2: Module Bays on Devices ---")
    bay_count = cleanup_bays(nb, dry)

    print("=" * 50)
    print(f"Total templates deleted: {tpl_count}")
    print(f"Total bays deleted: {bay_count}")

    if dry:
        print("\n=== DRY RUN complete --- re-run without --dry-run to execute ===")
    else:
        print("\nCleanup complete.")


if __name__ == "__main__":
    main()

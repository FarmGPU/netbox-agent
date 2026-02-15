#!/usr/bin/env python3
"""
One-time cleanup: delete bogus modules and module types created by the
first test run on ginger01 (before the CPU filter was hardened).

Deletes:
  - Modules with module_type model in ("4xxx Series QAT", "Intel Corporation")
  - Module types 24 ("4xxx Series QAT") and 25 ("Intel Corporation")
  - Empty CPU bays (CPU-2 through CPU-9) on device 211

Usage:
    NETBOX_URL=https://localhost NETBOX_TOKEN=<token> python3 cleanup_bad_modules.py [--dry-run]
"""

import argparse
import os
import sys

# Reuse the shared connection helper
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "schema"))
from nb_connection import get_api

BAD_MODULE_TYPE_MODELS = {"4xxx Series QAT", "Intel Corporation"}
# ASPEED was also auto-created as a module type (id=22) but has 0 instances
BAD_MODULE_TYPE_MODELS_EXTRA = {"ASPEED Graphics Family"}


def main():
    parser = argparse.ArgumentParser(description="Clean up bogus modules from test run")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be deleted")
    args = parser.parse_args()

    nb = get_api()
    dry = args.dry_run

    if dry:
        print("=== DRY RUN — no changes will be made ===\n")

    # 1. Delete bogus modules
    all_modules = list(nb.dcim.modules.all())
    deleted_modules = 0
    for mod in all_modules:
        mt_model = mod.module_type.model if mod.module_type else ""
        if mt_model in BAD_MODULE_TYPE_MODELS:
            print(f"DELETE module id={mod.id} bay={mod.module_bay.name} type={mt_model} device={mod.device.name}")
            if not dry:
                mod.delete()
            deleted_modules += 1

    print(f"\nModules deleted: {deleted_modules}")

    # 2. Delete bogus module types
    all_types = list(nb.dcim.module_types.all())
    deleted_types = 0
    for mt in all_types:
        if mt.model in BAD_MODULE_TYPE_MODELS or mt.model in BAD_MODULE_TYPE_MODELS_EXTRA:
            count = mt.module_count if hasattr(mt, "module_count") else "?"
            print(f"DELETE module_type id={mt.id} model={mt.model} instances={count}")
            if not dry:
                try:
                    mt.delete()
                except Exception as e:
                    print(f"  WARN: could not delete type {mt.model}: {e}")
            deleted_types += 1

    print(f"Module types deleted: {deleted_types}")

    # 3. Delete extra CPU bays (CPU-2 through CPU-9) on the test device
    # Find device by name pattern
    devices = list(nb.dcim.devices.filter(name__ic="ginger01"))
    deleted_bays = 0
    for device in devices:
        bays = list(nb.dcim.module_bays.filter(device_id=device.id))
        for bay in bays:
            if bay.name.startswith("CPU-"):
                idx = bay.name.split("-")[1]
                try:
                    if int(idx) >= 2:
                        print(f"DELETE module_bay id={bay.id} name={bay.name} device={device.name}")
                        if not dry:
                            bay.delete()
                        deleted_bays += 1
                except ValueError:
                    pass

    print(f"Module bays deleted: {deleted_bays}")

    if dry:
        print("\n=== DRY RUN complete — re-run without --dry-run to execute ===")
    else:
        print("\nCleanup complete. Re-run the agent with --update-modules to create correct data.")


if __name__ == "__main__":
    main()

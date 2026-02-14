#!/usr/bin/env python3
"""
08 — Orchestrator: run schema setup scripts 01-07 in sequence.

Usage:
    python 08_run_all_schema_setup.py                 # stop on first error
    python 08_run_all_schema_setup.py --continue-on-error  # run all, report failures at end
"""

import argparse
import importlib
import sys
import os
import traceback

sys.path.insert(0, os.path.dirname(__file__))
from nb_connection import get_api, logger

SCRIPTS = [
    ("01_delete_slurpit_custom_fields", "Delete slurpit custom fields"),
    ("02_create_custom_field_choice_sets", "Create custom field choice sets"),
    ("03_create_custom_fields", "Create custom fields"),
    ("04_create_module_type_profiles", "Create module type profiles"),
    ("05_create_module_types", "Create module types"),
    ("06_update_device_types_module_bay_templates", "Update device types with module bay templates"),
    ("07_create_spare_inventory_device", "Create spare inventory device"),
]


def main():
    parser = argparse.ArgumentParser(description="Run all schema setup scripts")
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue running remaining scripts even if one fails",
    )
    args = parser.parse_args()

    nb = get_api()
    failures = []

    for module_name, description in SCRIPTS:
        logger.info("=" * 60)
        logger.info("Running: %s — %s", module_name, description)
        logger.info("=" * 60)

        try:
            mod = importlib.import_module(module_name)
            mod.run(nb)
            logger.info("DONE: %s", module_name)
        except Exception as e:
            logger.error("FAILED: %s — %s", module_name, e)
            traceback.print_exc()
            failures.append((module_name, str(e)))
            if not args.continue_on_error:
                logger.error("Stopping due to error (use --continue-on-error to skip)")
                sys.exit(1)

    logger.info("=" * 60)
    if failures:
        logger.error("Schema setup completed with %d failure(s):", len(failures))
        for name, err in failures:
            logger.error("  %s: %s", name, err)
        sys.exit(1)
    else:
        logger.info("Schema setup completed successfully — all scripts passed")


if __name__ == "__main__":
    main()

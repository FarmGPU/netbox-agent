#!/usr/bin/env python3
"""
01 — Delete slurpit_* custom fields.

Removes the 7 leftover slurpit custom fields while preserving bmc_mac_address.
Idempotent: silently skips fields that don't exist.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from nb_connection import get_api, logger

SLURPIT_PREFIXES = ("slurpit_",)
KEEP_FIELDS = {"bmc_mac_address"}


def run(nb):
    custom_fields = list(nb.extras.custom_fields.all())
    deleted = 0

    for cf in custom_fields:
        if cf.name in KEEP_FIELDS:
            continue
        if any(cf.name.startswith(prefix) for prefix in SLURPIT_PREFIXES):
            logger.info("Deleting custom field: %s (id=%d)", cf.name, cf.id)
            cf.delete()
            deleted += 1

    if deleted == 0:
        logger.info("No slurpit custom fields found — nothing to delete")
    else:
        logger.info("Deleted %d slurpit custom field(s)", deleted)


def main():
    nb = get_api()
    run(nb)


if __name__ == "__main__":
    main()

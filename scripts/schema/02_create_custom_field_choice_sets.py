#!/usr/bin/env python3
"""
02 — Create custom field choice sets.

Creates OwnerChoices, EnvironmentChoices, and RecordCompletenessChoices.
Idempotent: skips choice sets that already exist.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from nb_connection import get_api, logger

CHOICE_SETS = [
    {
        "name": "OwnerChoices",
        "extra_choices": [
            ["FarmGPU", "FarmGPU"],
            ["Solidigm", "Solidigm"],
            ["Intel", "Intel"],
            ["Customer-Owned", "Customer-Owned"],
        ],
    },
    {
        "name": "EnvironmentChoices",
        "extra_choices": [
            ["Production", "Production"],
            ["Lab", "Lab"],
            ["Staging", "Staging"],
            ["Decomm", "Decomm"],
        ],
    },
    {
        "name": "RecordCompletenessChoices",
        "extra_choices": [
            ["complete", "complete"],
            ["incomplete", "incomplete"],
        ],
    },
]


def run(nb):
    for cs_def in CHOICE_SETS:
        existing = nb.extras.custom_field_choice_sets.get(name=cs_def["name"])
        if existing:
            logger.info("Choice set '%s' already exists (id=%d) — skipping", cs_def["name"], existing.id)
            continue

        result = nb.extras.custom_field_choice_sets.create(cs_def)
        logger.info("Created choice set '%s' (id=%d)", cs_def["name"], result.id)


def main():
    nb = get_api()
    run(nb)


if __name__ == "__main__":
    main()

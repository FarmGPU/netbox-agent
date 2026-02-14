#!/usr/bin/env python3
"""
04 — Update existing module type profiles and create missing ones.

Existing profiles (update schemas to be richer):
  CPU (id=1), GPU (id=3), Hard disk (id=4), Memory (id=5), Power supply (id=6)
Missing profiles (create):
  NIC

Idempotent: updates existing profiles with richer schemas, creates only what's missing.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from nb_connection import get_api, logger

# Map of profile name → desired schema
# These use the EXISTING names already in NetBox (not the plan's idealized names)
PROFILES = {
    "GPU": {
        "required": ["memory_gb", "form_factor"],
        "properties": {
            "memory_gb": {"type": "integer", "title": "Memory (GB)"},
            "form_factor": {
                "type": "string",
                "title": "Form Factor",
                "enum": ["SXM4", "SXM5", "PCIe"],
            },
            "tdp_watts": {"type": "integer", "title": "TDP (Watts)"},
        },
    },
    "CPU": {
        "properties": {
            "core_count": {"type": "integer", "title": "Core Count"},
            "base_frequency_ghz": {"type": "number", "title": "Base Frequency (GHz)"},
            "architecture": {
                "type": "string",
                "title": "Architecture",
                "enum": ["x86_64", "aarch64"],
            },
            "tdp_watts": {"type": "integer", "title": "TDP (Watts)"},
        },
    },
    "Hard disk": {
        "required": ["size"],
        "properties": {
            "size": {"type": "integer", "title": "Size (GB)", "description": "Raw disk capacity"},
            "type": {
                "enum": ["HD", "SSD", "NVME"],
                "type": "string",
                "title": "Disk type",
                "default": "NVME",
            },
            "form_factor": {
                "type": "string",
                "title": "Form Factor",
                "enum": ["E1.S", "E1.L", "U.2", "M.2", "2.5-inch", "3.5-inch"],
            },
            "interface": {
                "type": "string",
                "title": "Interface",
                "enum": ["Gen4", "Gen5", "SATA"],
            },
        },
    },
    "Memory": {
        "required": ["class", "size"],
        "properties": {
            "ecc": {"type": "boolean", "title": "ECC", "description": "Error-correcting code is enabled"},
            "size": {"type": "integer", "title": "Size (GB)", "description": "Raw capacity of the module"},
            "class": {
                "enum": ["DDR3", "DDR4", "DDR5"],
                "type": "string",
                "title": "Memory class",
                "default": "DDR5",
            },
            "data_rate": {"type": "integer", "title": "Data rate", "description": "Speed in MT/s"},
        },
    },
    "Power supply": {
        "required": ["input_current"],
        "properties": {
            "wattage": {"type": "integer", "description": "Available output power (watts)"},
            "hot_swappable": {"type": "boolean", "title": "Hot-swappable", "default": False},
            "input_current": {
                "enum": ["AC", "DC"],
                "type": "string",
                "title": "Current type",
                "default": "AC",
            },
            "input_voltage": {"type": "integer", "title": "Voltage", "default": 120},
            "efficiency": {
                "type": "string",
                "title": "Efficiency Rating",
                "enum": ["80+ Bronze", "80+ Silver", "80+ Gold", "80+ Platinum", "80+ Titanium"],
            },
        },
    },
    # NIC doesn't exist yet — will be created
    "NIC": {
        "properties": {
            "port_count": {"type": "integer", "title": "Port Count"},
            "speed_gbps": {
                "type": "string",
                "title": "Speed (Gbps)",
                "enum": ["1", "10", "25", "100", "200", "400"],
            },
            "form_factor": {
                "type": "string",
                "title": "Form Factor",
                "enum": ["PCIe", "OCP", "LOM"],
            },
        },
    },
}


def run(nb):
    for profile_name, schema in PROFILES.items():
        existing = nb.dcim.module_type_profiles.get(name=profile_name)
        if existing:
            # Update schema if it differs
            if existing.schema != schema:
                existing.schema = schema
                existing.save()
                logger.info("Updated profile '%s' (id=%d) with enriched schema", profile_name, existing.id)
            else:
                logger.info("Profile '%s' already has correct schema — skipping", profile_name)
        else:
            result = nb.dcim.module_type_profiles.create({
                "name": profile_name,
                "schema": schema,
            })
            logger.info("Created profile '%s' (id=%d)", profile_name, result.id)


def main():
    nb = get_api()
    run(nb)


if __name__ == "__main__":
    main()

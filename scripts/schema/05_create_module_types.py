#!/usr/bin/env python3
"""
05 — Update existing module types and seed known fleet types.

- Updates 6 existing GPU module types to assign the GPU profile and populate attributes
- Seeds known CPU, SSD, NIC, DIMM, PSU module types from fleet survey data
- Agent will auto-create unknown module types at runtime

Idempotent: skips module types that already have correct profile/attributes.
"""

import re
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from nb_connection import get_api, logger


def _get_or_create_manufacturer(nb, name):
    if not name:
        return None
    mfr = nb.dcim.manufacturers.get(name=name)
    if not mfr:
        # Name lookup failed — try slug-based lookup (handles casing differences)
        slug = re.sub(r"[^A-Za-z0-9]+", "-", name).lower().strip("-")
        mfr = nb.dcim.manufacturers.get(slug=slug)
        if mfr:
            logger.info("Manufacturer '%s' found by slug '%s' (actual name='%s')", name, slug, mfr.name)
        else:
            mfr = nb.dcim.manufacturers.create(name=name, slug=slug)
            logger.info("Created manufacturer '%s'", name)
    return mfr


def _get_profile(nb, name):
    profile = nb.dcim.module_type_profiles.get(name=name)
    if not profile:
        raise RuntimeError(f"Profile '{name}' not found — run 04 first")
    return profile


def _ensure_module_type(nb, manufacturer_name, model, profile, attribute_data=None):
    """Find or create a module type, ensuring profile is set."""
    mfr = _get_or_create_manufacturer(nb, manufacturer_name)

    existing = nb.dcim.module_types.get(manufacturer_id=mfr.id, model=model)

    if existing:
        update_needed = False
        existing_profile_id = None
        if hasattr(existing, "profile") and existing.profile:
            existing_profile_id = existing.profile.id if hasattr(existing.profile, "id") else existing.profile
        if existing_profile_id != profile.id:
            existing.profile = profile.id
            update_needed = True
        if attribute_data and getattr(existing, "attributes", None) != attribute_data:
            existing.attributes = attribute_data
            update_needed = True
        if update_needed:
            existing.save()
            logger.info("Updated module type '%s / %s'", manufacturer_name, model)
        else:
            logger.info("Module type '%s / %s' already correct — skipping", manufacturer_name, model)
        return existing

    create_params = {
        "manufacturer": mfr.id,
        "model": model,
        "profile": profile.id,
    }
    if attribute_data:
        create_params["attributes"] = attribute_data

    result = nb.dcim.module_types.create(create_params)
    logger.info("Created module type '%s / %s' (id=%d)", manufacturer_name, model, result.id)
    return result


# Existing GPU module types to update (these 6 already exist)
GPU_TYPES = [
    {"manufacturer": "NVIDIA", "model": "RTX 4000 Ada", "attributes": {"memory_gb": 20, "form_factor": "PCIe"}},
    {"manufacturer": "NVIDIA", "model": "RTX A5000", "attributes": {"memory_gb": 24, "form_factor": "PCIe"}},
    {"manufacturer": "NVIDIA", "model": "RTX 6000 Ada", "attributes": {"memory_gb": 48, "form_factor": "PCIe"}},
    {"manufacturer": "NVIDIA", "model": "RTX 4090", "attributes": {"memory_gb": 24, "form_factor": "PCIe"}},
    {"manufacturer": "NVIDIA", "model": "RTX A6000", "attributes": {"memory_gb": 48, "form_factor": "PCIe"}},
    {"manufacturer": "NVIDIA", "model": "H100 PCIe", "attributes": {"memory_gb": 80, "form_factor": "PCIe", "tdp_watts": 350}},
]

# Seed known fleet types
CPU_TYPES = [
    {"manufacturer": "Intel", "model": "Xeon Gold 6430", "attributes": {"core_count": 32, "base_frequency_ghz": 2.1, "architecture": "x86_64", "tdp_watts": 270}},
    {"manufacturer": "Intel", "model": "Xeon Gold 6448Y", "attributes": {"core_count": 32, "base_frequency_ghz": 2.1, "architecture": "x86_64", "tdp_watts": 225}},
    {"manufacturer": "Intel", "model": "Xeon Platinum 8480+", "attributes": {"core_count": 56, "base_frequency_ghz": 2.0, "architecture": "x86_64", "tdp_watts": 350}},
]

SSD_TYPES = [
    {"manufacturer": "Solidigm", "model": "D7-P5520 3.84TB", "attributes": {"size": 3840, "type": "NVME", "form_factor": "U.2", "interface": "Gen4"}},
    {"manufacturer": "Samsung", "model": "PM9A3 1.92TB", "attributes": {"size": 1920, "type": "NVME", "form_factor": "E1.S", "interface": "Gen4"}},
    {"manufacturer": "Samsung", "model": "PM9A3 3.84TB", "attributes": {"size": 3840, "type": "NVME", "form_factor": "U.2", "interface": "Gen4"}},
]

NIC_TYPES = [
    {"manufacturer": "Intel", "model": "E810-XXVDA2 25GbE", "attributes": {"port_count": 2, "speed_gbps": "25", "form_factor": "PCIe"}},
    {"manufacturer": "NVIDIA", "model": "ConnectX-6 Dx 100GbE", "attributes": {"port_count": 2, "speed_gbps": "100", "form_factor": "PCIe"}},
    {"manufacturer": "Broadcom", "model": "BCM57416 OCP 25GbE", "attributes": {"port_count": 2, "speed_gbps": "25", "form_factor": "OCP"}},
]

DIMM_TYPES = [
    {"manufacturer": "Samsung", "model": "M393A8G40AB2-CWE 64GB", "attributes": {"size": 64, "class": "DDR4", "data_rate": 3200, "ecc": True}},
    {"manufacturer": "Samsung", "model": "M321R8GA0BB0-CQKZJ 64GB", "attributes": {"size": 64, "class": "DDR5", "data_rate": 4800, "ecc": True}},
    {"manufacturer": "SK Hynix", "model": "HMCG94AEBRA109N 64GB", "attributes": {"size": 64, "class": "DDR5", "data_rate": 4800, "ecc": True}},
]

PSU_TYPES = [
    {"manufacturer": "Delta", "model": "DPS-2400AB 2400W", "attributes": {"wattage": 2400, "input_current": "AC", "efficiency": "80+ Platinum"}},
    {"manufacturer": "Liteon", "model": "PS-2162-5L 1600W", "attributes": {"wattage": 1600, "input_current": "AC", "efficiency": "80+ Platinum"}},
]


def run(nb):
    gpu_profile = _get_profile(nb, "GPU")
    cpu_profile = _get_profile(nb, "CPU")
    disk_profile = _get_profile(nb, "Hard disk")
    nic_profile = _get_profile(nb, "NIC")
    memory_profile = _get_profile(nb, "Memory")
    psu_profile = _get_profile(nb, "Power supply")

    for spec in GPU_TYPES:
        _ensure_module_type(nb, spec["manufacturer"], spec["model"], gpu_profile, spec.get("attributes"))
    for spec in CPU_TYPES:
        _ensure_module_type(nb, spec["manufacturer"], spec["model"], cpu_profile, spec.get("attributes"))
    for spec in SSD_TYPES:
        _ensure_module_type(nb, spec["manufacturer"], spec["model"], disk_profile, spec.get("attributes"))
    for spec in NIC_TYPES:
        _ensure_module_type(nb, spec["manufacturer"], spec["model"], nic_profile, spec.get("attributes"))
    for spec in DIMM_TYPES:
        _ensure_module_type(nb, spec["manufacturer"], spec["model"], memory_profile, spec.get("attributes"))
    for spec in PSU_TYPES:
        _ensure_module_type(nb, spec["manufacturer"], spec["model"], psu_profile, spec.get("attributes"))


def main():
    nb = get_api()
    run(nb)


if __name__ == "__main__":
    main()

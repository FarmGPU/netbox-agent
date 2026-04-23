"""
Pre-flight dependency checker for netbox-agent.

Validates that required system tools are available before the agent
attempts to use them.  Does NOT import netbox_agent.config to avoid
module-load-time side effects that complicate testing.
"""

import logging
from shutil import which

logger = logging.getLogger("netbox_agent.dependencies")

# tool_name -> (description, required)
TOOLS = {
    "dmidecode":  ("DMI/SMBIOS data (serial, chassis, PSUs, DIMMs)", True),
    "lshw":       ("Hardware tree (GPUs, NICs, storage fallback)", True),
    "lsblk":      ("Block device enumeration (primary storage)", False),
    "lscpu":      ("CPU socket detection (primary CPU path)", False),
    "ipmitool":   ("IPMI/BMC data (OOB IP, MAC, asset tag)", False),
    "ethtool":    ("NIC speed/duplex detection", False),
    "lldpctl":    ("LLDP neighbor discovery (auto-cabling)", False),
    "nvme":       ("NVMe device enrichment (vendor, firmware)", False),
    "nvidia-smi": ("NVIDIA GPU serial numbers", False),
}


def check_all():
    """Return {tool_name: bool} for every tool in TOOLS."""
    return {name: which(name) is not None for name in TOOLS}


def get_missing():
    """Return list of tool names that are not on PATH."""
    return [name for name, available in check_all().items() if not available]


def missing_deps_string(avail):
    """Build a comma-separated string of missing tools from an availability dict."""
    missing = [name for name, present in avail.items() if not present]
    return ", ".join(sorted(missing)) if missing else ""


def log_status():
    """Log availability of every tool — warnings for required, debug for optional."""
    avail = check_all()
    for name, present in avail.items():
        desc, required = TOOLS[name]
        if present:
            logger.debug("  [OK]   %-12s %s", name, desc)
        elif required:
            logger.warning("  [MISS] %-12s %s (REQUIRED)", name, desc)
        else:
            logger.info("  [MISS] %-12s %s (optional)", name, desc)
    return avail

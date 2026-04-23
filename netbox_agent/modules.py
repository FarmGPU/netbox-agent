"""
ModuleManager — Manages hardware components as NetBox Modules.

Replaces inventory.py functionality by using the Modules API instead of
Inventory Items. Supports re-parenting, spare inventory tracking, and
module type auto-creation with typed profiles.
"""

import json
import logging
import re
import subprocess
import time
from pathlib import Path

from netbox_agent.config import netbox_instance as nb
from netbox_agent.lshw import LSHW
from netbox_agent.misc import is_tool

logger = logging.getLogger("netbox_agent.modules")

# Categories mapped to their bay prefix and profile name
# Profile names must match the actual names in NetBox (created by script 04)
CATEGORIES = {
    "cpu": {"prefix": "CPU", "profile": "CPU"},
    "gpu": {"prefix": "GPU", "profile": "GPU"},
    "accelerator": {"prefix": "ACC", "profile": "Other"},
    "dimm": {"prefix": "DIMM", "profile": "Memory"},
    "ssd": {"prefix": "SSD", "profile": "Hard disk"},
    "nic": {"prefix": "NIC", "profile": "NIC"},
    "psu": {"prefix": "PSU", "profile": "Power supply"},
}

# Retry settings for API calls
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds, doubles each retry


def _api_retry(func, *args, **kwargs):
    """Execute an API call with exponential backoff retry."""
    for attempt in range(MAX_RETRIES):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF * (2 ** attempt)
                logger.warning("API call failed (%s), retrying in %ds...", e, wait)
                time.sleep(wait)
            else:
                raise


class ModuleManager:
    """
    Manages hardware modules for a single device.

    Detects local hardware via lshw/dmidecode/nvidia-smi, resolves module types,
    ensures module bays exist, and syncs the state to NetBox.
    """

    def __init__(self, server, config):
        """
        Args:
            server: ServerBase instance (provides device info, dmi data)
            config: Parsed configuration namespace
        """
        self.server = server
        self.config = config
        self.lshw = LSHW()
        self.device = None  # NetBox device record, set during sync
        self.default_owner = getattr(config.device, "default_owner", "FarmGPU")

        # Caches to reduce API calls
        self._profile_cache = {}
        self._manufacturer_cache = {}
        self._module_type_cache = {}
        self._spare_device = None

    # ------------------------------------------------------------------ #
    #  Hardware Detection
    # ------------------------------------------------------------------ #

    # Vendor ID normalization (from SILO cpu.py)
    _INTEL_ALIASES = {"genuineintel", "intel", "intel(r) corporation", "intel corporation", "intel corp."}
    _AMD_ALIASES = {"authenticamd", "advanced micro devices", "amd", "advanced micro devices [amd]"}

    def _normalize_cpu_vendor(self, vendor_id):
        """Normalize CPU vendor string. Informed by SILO's normalize_cpu_make()."""
        if not vendor_id:
            return "Unknown"
        normalized = vendor_id.strip().lower()
        if normalized in self._INTEL_ALIASES:
            return "Intel"
        if normalized in self._AMD_ALIASES:
            return "AMD"
        return vendor_id.strip()

    def _get_local_cpus(self):
        """
        Detect CPUs via lscpu (primary) or lshw (fallback).

        Uses lscpu -J as the primary source — it only reports actual CPU
        sockets, never QAT/DLB/IAA/DSA accelerators. Informed by SILO's
        cpu.py parser which uses the same approach.
        """
        lscpu_data = self._run_lscpu()
        if lscpu_data is not None:
            return self._parse_lscpu(lscpu_data)
        # Fallback to lshw (less reliable — includes accelerators as "processor")
        logger.warning("lscpu not available, falling back to lshw for CPU detection")
        return self._get_local_cpus_lshw_fallback()

    def _run_lscpu(self):
        """Run lscpu -J and return parsed JSON, or None on failure."""
        if not is_tool("lscpu"):
            return None
        try:
            output = subprocess.check_output(
                ["lscpu", "-J"],
                encoding="utf-8",
                timeout=30,
            )
            return json.loads(output)
        except Exception as e:
            logger.warning("lscpu -J failed: %s", e)
            return None

    def _parse_lscpu(self, lscpu_data):
        """
        Parse lscpu -J output into CPU items for NetBox modules.
        Informed by SILO's cpu.py parse() function.
        """
        # Build field lookup from lscpu entries
        fields = {}
        for entry in lscpu_data.get("lscpu", []):
            field_raw = (entry.get("field") or "").strip()
            if field_raw.endswith(":"):
                field_raw = field_raw[:-1].strip()
            if field_raw:
                fields[field_raw] = entry.get("data")

        sockets = int(fields.get("Socket(s)", 0) or 0)
        model = fields.get("Model name") or fields.get("Model") or "Unknown CPU"
        vendor_id = fields.get("Vendor ID") or fields.get("Vendor") or "Unknown"
        vendor = self._normalize_cpu_vendor(vendor_id)

        if sockets < 1:
            sockets = 1

        items = []
        for i in range(sockets):
            items.append({
                "product": model.strip(),
                "vendor": vendor,
                "serial": None,  # CPUs don't report serials
                "slot": f"CPU{i}",
            })
        return items

    def _get_local_cpus_lshw_fallback(self):
        """Fallback CPU detection via lshw (used only when lscpu unavailable)."""
        items = []
        for cpu in self.lshw.get_hw_linux("cpu"):
            product = cpu.get("product", "Unknown CPU")
            vendor = cpu.get("vendor", "Unknown")
            description = cpu.get("description", "")

            # Basic filtering for lshw fallback — skip obvious accelerators
            combined = f"{product} {description}".lower()
            skip_keywords = {"qat", "quickassist", "dlb", "iaa", "dsa",
                             "co-processor", "coprocessor", "accelerator", "4xxx"}
            if any(kw in combined for kw in skip_keywords):
                continue
            # Skip entries where product is just a vendor name
            if product.lower().strip() in self._INTEL_ALIASES | self._AMD_ALIASES:
                continue

            items.append({
                "product": product,
                "vendor": self._normalize_cpu_vendor(vendor),
                "serial": None,
                "slot": cpu.get("location", ""),
            })
        return items

    # BMC/onboard VGA and baseboard management controllers that should NOT be tracked as GPU modules
    _SKIP_GPU_VENDORS = {
        "aspeed technology, inc.",
        "matrox electronics systems ltd.",
        "intelligent platform management interface (ipmi) forum (intel, hp, nec, dell)",
    }
    _SKIP_GPU_KEYWORDS = {"aspeed", "matrox", "vga compatible", "ipmi", "pnp device ipi"}

    def _get_local_gpus(self):
        """Detect GPUs via lshw + vendor tools for serials and driver info.

        Supports NVIDIA (nvidia-smi), AMD (amdgpu sysfs), and Intel (i915 sysfs).
        Filters out BMC VGA controllers. Includes driver version in description.
        """
        gpus = self.lshw.get_hw_linux("gpu")

        # Detect vendor-specific driver, runtime, and serial info
        nvidia_driver, nvidia_cuda, nvidia_gpu_info = self._get_nvidia_gpu_info()
        amd_driver = self._get_amd_gpu_driver()
        amd_rocm = self._get_amd_rocm_version()
        amd_serials = self._get_amd_gpu_serials() if amd_driver else {}
        amd_gpu_idx = 0  # Counter for AMD GPUs (separate from NVIDIA index)
        gaudi_driver, gaudi_devices = self._get_intel_gaudi_info()
        gaudi_serials = {d.get("businfo", ""): d.get("serial") for d in gaudi_devices if d.get("serial")}
        gaudi_names = {d.get("businfo", ""): d.get("product") for d in gaudi_devices if d.get("product")}

        items = []
        real_idx = 0  # index into nvidia-smi GPU info (only real GPUs)
        for gpu in gpus:
            product = gpu.get("product", "Unknown GPU")
            vendor = gpu.get("vendor", "Unknown")
            vendor_lower = vendor.lower()
            description = gpu.get("description", "")
            businfo = gpu.get("businfo", "")

            # Skip BMC/onboard VGA controllers
            if vendor_lower in self._SKIP_GPU_VENDORS:
                logger.debug("Skipping onboard VGA: %s %s", vendor, product)
                continue
            if any(kw in product.lower() for kw in self._SKIP_GPU_KEYWORDS):
                logger.debug("Skipping onboard VGA: %s", product)
                continue
            # Skip onboard VGA — but NOT discrete NVIDIA/AMD GPUs which may also
            # report as "VGA compatible" when lshw doesn't have the PCI ID mapping.
            is_known_gpu_vendor = any(v in vendor_lower for v in ("nvidia", "amd", "ati"))
            if "VGA compatible" in description and "3D" not in description and not is_known_gpu_vendor:
                logger.debug("Skipping VGA-only device: %s %s", vendor, product)
                continue

            # Resolve serial, product name, and driver per vendor
            serial = None
            driver = ""

            if "nvidia" in vendor_lower:
                nv_info = nvidia_gpu_info.get(real_idx, {})
                serial = nv_info.get("serial")
                driver = nvidia_driver
                # Always prefer nvidia-smi product name over lshw. lshw may
                # return vendor name ("NVIDIA Corporation"), chip codename
                # ("GA103", "AD102GL"), or truncated names when its PCI ID
                # database is outdated. nvidia-smi is authoritative.
                nv_name = nv_info.get("name", "")
                if nv_name:
                    product = nv_name
                real_idx += 1
            elif "amd" in vendor_lower or "ati" in vendor_lower:
                driver = amd_driver
                serial = amd_serials.get(amd_gpu_idx)
                amd_gpu_idx += 1
            elif "habana" in vendor_lower or "gaudi" in product.lower():
                # Intel Gaudi — enriched from hl-smi
                driver = gaudi_driver
                # Match by PCI bus address for serial and product name
                bus_addr = businfo.split("@")[-1] if "@" in businfo else businfo
                serial = gaudi_serials.get(bus_addr)
                hl_name = gaudi_names.get(bus_addr)
                if hl_name:
                    product = hl_name
                vendor = "Habana Labs (Intel)"
            else:
                # Generic: read driver from sysfs for this PCI device
                driver = self._get_driver_for_pci_device(businfo)

            # Build description: driver + compute runtime (CUDA/ROCm/Habana)
            desc_parts = []
            if driver:
                desc_parts.append(f"driver: {driver}")
            if "nvidia" in vendor_lower and nvidia_cuda:
                desc_parts.append(f"CUDA: {nvidia_cuda}")
            elif ("amd" in vendor_lower or "ati" in vendor_lower) and amd_rocm:
                desc_parts.append(f"ROCm: {amd_rocm}")
            elif "habana" in vendor_lower and gaudi_driver:
                desc_parts.append(f"SynapseAI: habanalabs {gaudi_driver}")
            full_desc = " | ".join(p for p in desc_parts if p)

            items.append({
                "product": product,
                "vendor": vendor,
                "serial": serial,
                "description": full_desc,
            })

        # Intel Gaudi: lshw classifies these as "network" (not "display"),
        # so they don't appear in lshw.gpus. Add them from hl-smi if not
        # already found via the lshw GPU loop above.
        if gaudi_devices:
            existing_serials = {g.get("serial") for g in items if g.get("serial")}
            for gdev in gaudi_devices:
                if gdev.get("serial") and gdev["serial"] not in existing_serials:
                    desc_parts = []
                    if gaudi_driver:
                        desc_parts.append(f"driver: habanalabs {gaudi_driver}")
                        desc_parts.append(f"SynapseAI: habanalabs {gaudi_driver}")
                    items.append({
                        "product": gdev.get("product", "Gaudi GPU"),
                        "vendor": "Habana Labs (Intel)",
                        "serial": gdev.get("serial"),
                        "description": " | ".join(desc_parts),
                    })

        return items

    def _get_local_accelerators(self):
        """Detect non-GPU compute accelerators (Gaudi, FPGA, DPU, etc.).

        Uses lshw for PCI device discovery, then enriches with vendor-specific
        tools (hl-smi for Gaudi) when available. Non-destructive — never
        installs drivers, only reads what's present.

        Returns list of dicts compatible with the Modules API.
        """
        items = []

        # Source 1: lshw accelerator class devices (coprocessor, generic, processing)
        lshw_accs = self.lshw.get_hw_linux("accelerator")
        for acc in lshw_accs:
            product = acc.get("product", "Unknown Accelerator")
            vendor = acc.get("vendor", "Unknown")
            businfo = acc.get("businfo", "")
            description = acc.get("description", "")

            # Get driver info from sysfs
            driver = self._get_driver_for_pci_device(businfo)

            desc_parts = [description] if description else []
            if driver:
                desc_parts.append(f"driver: {driver}")

            items.append({
                "product": product,
                "vendor": vendor,
                "serial": None,
                "description": " | ".join(p for p in desc_parts if p),
                "businfo": businfo,
            })

        # Note: Intel Gaudi (Habana Labs) is now routed to GPUs via lshw.py,
        # not to accelerators. Gaudi enrichment (hl-smi serials, driver) happens
        # in _get_local_gpus(). This method only handles non-GPU accelerators
        # (Pliops, FPGAs, QAT, custom hardware).

        if items:
            logger.info("Detected %d accelerator(s): %s",
                        len(items),
                        ", ".join(f'{a["vendor"]} {a["product"]}' for a in items))

        return items

    def _get_nvidia_gpu_info(self):
        """Query nvidia-smi for GPU names, serial numbers, driver, and CUDA version.

        Returns:
            (driver_version, cuda_version, {index: {"serial": str, "name": str}})
        """
        driver = ""
        cuda = ""
        gpu_info = {}
        if not is_tool("nvidia-smi"):
            return driver, cuda, gpu_info
        try:
            output = subprocess.check_output(
                ["nvidia-smi", "--query-gpu=index,name,serial,driver_version",
                 "--format=csv,noheader,nounits"],
                encoding="utf-8",
                timeout=30,
            ).strip()
            for line in output.splitlines():
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 4:
                    idx = int(parts[0])
                    name = parts[1]
                    sn = parts[2]
                    drv = parts[3]
                    info = {"name": name, "serial": None}
                    if sn and sn not in ("[N/A]", "N/A", "0", ""):
                        info["serial"] = sn
                    gpu_info[idx] = info
                    if drv and drv not in ("[N/A]", "N/A", ""):
                        driver = drv
        except Exception as e:
            logger.warning("nvidia-smi query failed: %s", e)

        # CUDA version from nvidia-smi header (not available via --query-gpu)
        try:
            import re
            header = subprocess.check_output(
                ["nvidia-smi"], encoding="utf-8", timeout=10,
            )
            m = re.search(r"CUDA Version:\s*([0-9.]+)", header)
            if m:
                cuda = m.group(1)
        except Exception:
            pass

        return driver, cuda, gpu_info

    def _get_amd_rocm_version(self):
        """Get AMD ROCm version if installed. Analogous to CUDA for NVIDIA."""
        # Try rocm-smi
        if is_tool("rocm-smi"):
            try:
                output = subprocess.check_output(
                    ["rocm-smi", "--showdriverversion"],
                    encoding="utf-8", timeout=10,
                ).strip()
                for line in output.splitlines():
                    if "Driver version" in line:
                        return line.split(":")[-1].strip()
            except Exception:
                pass
        # Try rocminfo
        if is_tool("rocminfo"):
            try:
                output = subprocess.check_output(
                    ["rocminfo"], encoding="utf-8", timeout=10,
                ).strip()
                for line in output.splitlines():
                    if "Runtime Version" in line:
                        return line.split(":")[-1].strip()
            except Exception:
                pass
        # Try /opt/rocm/.info/version
        try:
            with open("/opt/rocm/.info/version") as f:
                return f.read().strip()
        except (FileNotFoundError, PermissionError):
            pass
        return ""

    def _get_amd_gpu_driver(self):
        """Get AMD GPU driver version from sysfs or modinfo."""
        # Try sysfs first (most reliable)
        try:
            with open("/sys/module/amdgpu/version") as f:
                return f.read().strip()
        except (FileNotFoundError, PermissionError):
            pass
        # Fallback to modinfo
        try:
            output = subprocess.check_output(
                ["modinfo", "amdgpu", "-F", "version"],
                encoding="utf-8", timeout=10, stderr=subprocess.DEVNULL,
            ).strip()
            if output:
                return output.splitlines()[0]
        except Exception:
            pass
        return ""

    def _get_amd_gpu_serials(self):
        """Get AMD GPU serial numbers from rocm-smi or sysfs.

        AMD Instinct (MI200, MI300) and some consumer GPUs expose serials.
        Returns {gpu_index: serial}.
        """
        serials = {}

        # Method 1: rocm-smi --showserial
        if is_tool("rocm-smi"):
            try:
                import re
                output = subprocess.check_output(
                    ["rocm-smi", "--showserial"],
                    encoding="utf-8", timeout=15, stderr=subprocess.DEVNULL,
                ).strip()
                # Parse "GPU[0] : Serial Number: XXXX" format
                for m in re.finditer(r"GPU\[(\d+)\]\s*:\s*Serial Number:\s*(\S+)", output):
                    idx = int(m.group(1))
                    sn = m.group(2)
                    if sn and sn not in ("N/A", "0", ""):
                        serials[idx] = sn
                return serials
            except Exception as e:
                logger.debug("rocm-smi serial query failed: %s", e)

        # Method 2: sysfs serial_number (kernel 5.15+)
        try:
            import os
            import glob
            for card_dir in sorted(glob.glob("/sys/class/drm/card[0-9]*/device/")):
                serial_path = os.path.join(card_dir, "serial_number")
                vendor_path = os.path.join(card_dir, "vendor")
                try:
                    with open(vendor_path) as f:
                        vendor_id = f.read().strip()
                    # AMD vendor ID = 0x1002
                    if vendor_id != "0x1002":
                        continue
                    with open(serial_path) as f:
                        sn = f.read().strip()
                    if sn and sn not in ("0", "0x0000000000000000"):
                        # Extract card index from path
                        card_name = card_dir.split("/")[-3]  # "card0"
                        idx = int(card_name.replace("card", ""))
                        serials[idx] = sn
                except (FileNotFoundError, ValueError):
                    continue
        except Exception as e:
            logger.debug("sysfs AMD serial lookup failed: %s", e)

        return serials

    def _get_driver_for_pci_device(self, businfo: str) -> str:
        """Get the kernel driver bound to a PCI device.

        Args:
            businfo: lshw businfo field, e.g., "pci@0000:41:00.0"

        Returns:
            Driver name and version string, e.g., "amdgpu 6.7.0" or "habanalabs 1.17.0"
        """
        if not businfo:
            return ""
        # Extract PCI address from "pci@0000:41:00.0" format
        pci_addr = businfo.split("@")[-1] if "@" in businfo else businfo
        # Check sysfs for the driver
        driver_link = f"/sys/bus/pci/devices/{pci_addr}/driver"
        try:
            import os
            driver_path = os.readlink(driver_link)
            driver_name = os.path.basename(driver_path)
            # Get driver version from modinfo
            try:
                version = subprocess.check_output(
                    ["modinfo", driver_name, "-F", "version"],
                    encoding="utf-8", timeout=10, stderr=subprocess.DEVNULL,
                ).strip().splitlines()[0]
                return f"{driver_name} {version}"
            except Exception:
                return driver_name
        except (FileNotFoundError, OSError):
            return ""

    def _get_intel_gaudi_info(self):
        """Detect Intel Gaudi (Habana Labs) accelerators and their driver.

        Uses hl-smi if available, otherwise falls back to sysfs/modinfo
        for the habanalabs kernel module.

        Returns:
            (driver_version, [{serial, product, ...}])
        """
        driver = ""
        devices = []

        # Check for habanalabs kernel module
        try:
            with open("/sys/module/habanalabs/version") as f:
                driver = f.read().strip()
        except (FileNotFoundError, PermissionError):
            # Try modinfo
            try:
                output = subprocess.check_output(
                    ["modinfo", "habanalabs", "-F", "version"],
                    encoding="utf-8", timeout=10, stderr=subprocess.DEVNULL,
                ).strip()
                if output:
                    driver = output.splitlines()[0]
            except Exception:
                pass

        # Use hl-smi for device details if available
        if is_tool("hl-smi"):
            try:
                output = subprocess.check_output(
                    ["hl-smi", "-Q", "index,name,serial,bus_id", "-f", "csv,noheader"],
                    encoding="utf-8", timeout=30,
                ).strip()
                for line in output.splitlines():
                    parts = [p.strip() for p in line.split(",")]
                    if len(parts) >= 4:
                        devices.append({
                            "index": int(parts[0]),
                            "product": parts[1],
                            "serial": parts[2] if parts[2] not in ("N/A", "") else None,
                            "businfo": parts[3],
                        })
            except Exception as e:
                logger.debug("hl-smi query failed: %s", e)

        return driver, devices

    def _get_local_dimms(self):
        """Detect DIMMs via lshw memory children."""
        items = []
        for dimm in self.lshw.memories:
            serial = dimm.get("serial", "N/A")
            if serial in ("N/A", "NO DIMM", "Not Specified", "Unknown"):
                serial = None
            size_gb = dimm.get("size", 0)
            if isinstance(size_gb, (int, float)):
                size_gb = int(size_gb)
            product = dimm.get("product", "Unknown")
            items.append({
                "product": f"{product} {size_gb}GB" if size_gb else product,
                "vendor": dimm.get("vendor", "Unknown"),
                "serial": serial,
                "slot": dimm.get("slot", ""),
                "size_gb": size_gb,
                "description": dimm.get("description", ""),
            })
        return items

    # Device names that are never physical storage
    _SKIP_STORAGE_NAMES = {"loop", "ram", "zram", "dm-", "md", "nbd", "sr"}

    def _get_local_ssds(self):
        """
        Detect all physical storage devices via lsblk + nvme list.

        Uses lsblk as the primary source (handles NVMe, SATA, SAS, USB).
        Supplements NVMe devices with nvme-cli for firmware/vendor details.
        Falls back to lshw if lsblk is unavailable.
        """
        lsblk_data = self._run_lsblk()
        if lsblk_data is not None:
            return self._parse_lsblk_storage(lsblk_data)
        # Fallback: original lshw-based detection
        return self._get_local_ssds_lshw_fallback()

    def _run_lsblk(self):
        """Run lsblk -J -b and return parsed JSON, or None on failure."""
        if not is_tool("lsblk"):
            logger.warning("lsblk not found — falling back to lshw for storage")
            return None
        try:
            columns = "NAME,TYPE,SIZE,MODEL,SERIAL,VENDOR,TRAN,ROTA,HCTL,REV"
            output = subprocess.check_output(
                ["lsblk", "-J", "-b", "-o", columns],
                encoding="utf-8",
                timeout=30,
            )
            return json.loads(output)
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            # Try minimal columns (older kernels may not support all)
            try:
                output = subprocess.check_output(
                    ["lsblk", "-J", "-b", "-o", "NAME,TYPE,SIZE,MODEL,SERIAL,VENDOR,TRAN,ROTA"],
                    encoding="utf-8",
                    timeout=30,
                )
                return json.loads(output)
            except Exception:
                logger.warning("lsblk failed: %s", e)
                return None
        except Exception as e:
            logger.warning("lsblk failed: %s", e)
            return None

    def _run_nvme_list(self):
        """Run nvme list -o json and return parsed data, or None on failure."""
        if not is_tool("nvme"):
            return None
        try:
            output = subprocess.check_output(
                ["nvme", "list", "-o", "json"],
                encoding="utf-8",
                timeout=30,
            )
            return json.loads(output)
        except Exception as e:
            logger.debug("nvme list failed: %s", e)
            return None

    @staticmethod
    def _read_sysfs(path):
        """Read a sysfs file and return stripped content, or None on failure."""
        try:
            return Path(path).read_text().strip() or None
        except (OSError, IOError):
            return None

    def _parse_lsblk_storage(self, lsblk_data):
        """
        Parse lsblk JSON output into storage items for NetBox modules.

        Filters to physical disks only (TYPE=disk), excludes virtual/loop/ram
        devices, and enriches NVMe entries with nvme-cli data.
        """
        items = []
        seen_serials = set()

        # Build NVMe lookup from nvme-cli for richer data
        nvme_by_name = {}
        nvme_data = self._run_nvme_list()
        if nvme_data:
            for dev in nvme_data.get("Devices", []):
                dev_path = dev.get("DevicePath", "")
                # /dev/nvme0n1 → nvme0n1
                dev_name = dev_path.replace("/dev/", "")
                nvme_by_name[dev_name] = dev

        blockdevices = lsblk_data.get("blockdevices", [])
        for blk in blockdevices:
            dtype = (blk.get("type") or "").lower()
            name = blk.get("name") or ""

            # Only physical disks
            if dtype != "disk":
                continue

            # Skip virtual/loop/ram/device-mapper
            if any(name.startswith(prefix) for prefix in self._SKIP_STORAGE_NAMES):
                continue

            serial = (blk.get("serial") or "").strip() or None
            model = (blk.get("model") or "").strip() or None
            vendor = (blk.get("vendor") or "").strip() or None
            tran = (blk.get("tran") or "").strip().lower()
            rota = blk.get("rota")  # 0=SSD/NVMe, 1=HDD
            size_bytes = blk.get("size")
            rev = (blk.get("rev") or "").strip() or None

            # Treat placeholder serials as missing.
            # "SSN" / "ModelNumber" appear as literal values when an NVMe
            # drive's identify response is malformed (failed drive returns
            # the spec field names instead of values). Combined with the
            # "no model and no serial → skip" rule below, this filters out
            # drives that can't be reliably identified.
            if serial in ("_", "0", "unknown", "N/A", "UNKNOWN", "SSN"):
                serial = None
            if model == "ModelNumber":
                model = None

            # For NVMe devices, read identity from sysfs controller
            # (more reliable than lsblk — works even when NVMe char device is locked)
            if name.startswith("nvme") and "n" in name:
                ctrl_name = name.split("n")[0]  # nvme0n1 → nvme0
                sysfs_ctrl = Path(f"/sys/class/nvme/{ctrl_name}")
                if sysfs_ctrl.exists():
                    sysfs_serial = self._read_sysfs(sysfs_ctrl / "serial")
                    sysfs_model = self._read_sysfs(sysfs_ctrl / "model")
                    sysfs_fw = self._read_sysfs(sysfs_ctrl / "firmware_rev")
                    if sysfs_serial:
                        serial = sysfs_serial
                    if sysfs_model and not model:
                        model = sysfs_model
                    if sysfs_fw and not rev:
                        rev = sysfs_fw

            # Skip devices with no model AND no serial (virtual/unknown)
            if not model and not serial:
                continue

            # Dedup by serial
            if serial and serial in seen_serials:
                continue
            if serial:
                seen_serials.add(serial)

            # Enrich NVMe devices with nvme-cli data (optional, may fail on locked drives)
            nvme_info = nvme_by_name.get(name)
            if nvme_info:
                if not model:
                    model = (nvme_info.get("ModelNumber") or "").strip() or None
                if not serial:
                    serial = (nvme_info.get("SerialNumber") or "").strip() or None
                if not vendor:
                    vendor = (nvme_info.get("Vendor") or nvme_info.get("Manufacturer") or "").strip() or None
                if not size_bytes:
                    size_bytes = nvme_info.get("PhysicalSize") or nvme_info.get("UsedBytes")
                if not rev:
                    rev = nvme_info.get("Firmware")

            # Guess vendor from model if still unknown
            if not vendor and model:
                vendor = self._guess_vendor(model)

            # Detect interface from TRAN field
            interface = self._detect_storage_interface(tran, name)

            # Build description from interface + rotational status
            description = self._build_storage_description(interface, rota)

            items.append({
                "product": model or f"Unknown ({name})",
                "vendor": vendor or "Unknown",
                "serial": serial,
                "description": description,
                "interface": interface,
                "size_bytes": int(size_bytes) if size_bytes else None,
                "firmware": rev,
                "name": name,
            })

        return items

    def _detect_storage_interface(self, tran, name):
        """
        Detect storage interface from lsblk TRAN field and device name.
        Informed by SILO's detect_storage_interface() logic.
        """
        if tran:
            tran_map = {
                "nvme": "NVMe",
                "sata": "SATA",
                "sas": "SAS",
                "usb": "USB",
                "ata": "SATA",
                "fc": "FC",
                "iscsi": "iSCSI",
            }
            if tran in tran_map:
                return tran_map[tran]
            return tran.upper()

        # Infer from device name (fallback)
        if name.startswith("nvme"):
            return "NVMe"
        if name.startswith("sd"):
            return "SATA"  # Could be SAS, but SATA is more common
        if name.startswith("hd"):
            return "IDE"
        return None

    def _build_storage_description(self, interface, rota):
        """Build a human-readable storage description."""
        parts = []
        if interface:
            parts.append(interface)
        if rota is not None:
            if rota == "1" or rota is True or rota == 1:
                parts.append("HDD")
            else:
                parts.append("SSD")
        else:
            parts.append("disk")
        return " ".join(parts) if parts else "disk"

    def _get_local_ssds_lshw_fallback(self):
        """Original lshw-based storage detection (fallback when lsblk unavailable)."""
        items = []
        seen_serials = set()

        for disk in self.lshw.get_hw_linux("storage"):
            serial = disk.get("serial")
            product = disk.get("product")
            if not product:
                continue
            desc = (disk.get("description") or "").lower()
            if any(kw in desc for kw in ("volume", "virtual", "dvd-ram", "logical")):
                continue
            if serial and serial in seen_serials:
                continue
            if serial:
                seen_serials.add(serial)

            vendor = disk.get("vendor")
            if not vendor and product:
                vendor = self._guess_vendor(product)

            items.append({
                "product": product,
                "vendor": vendor or "Unknown",
                "serial": serial,
                "description": disk.get("description", ""),
            })

        return items

    # Vendors whose lshw "network" class devices are actually GPUs.
    # Habana Gaudi cards appear as class=network in lshw (because they
    # expose ethernet ports) but are actually GPUs in lspci ("Processing
    # accelerators"). We skip these in NIC detection — they're handled
    # as GPUs in _get_local_gpus() via the hl-smi enrichment path.
    _GPU_AS_NIC_VENDORS = {"habana labs"}

    def _get_local_nics(self):
        """
        Detect physical NIC cards via lshw, grouped by PCI bus address.

        Multi-port cards (e.g., dual-port ConnectX-7) share a PCI bus prefix
        and are reported as a single module using the first port's MAC as serial.
        InfiniBand interfaces (GUID > 6 bytes) are filtered out.
        """
        items = []
        seen_pci_cards = set()  # Track PCI card addresses (bus:slot, without function)

        for iface in self.lshw.interfaces:
            mac = iface.get("serial", iface.get("macaddress", ""))
            product = iface.get("product", "Unknown NIC")
            vendor = iface.get("vendor", "Unknown")
            businfo = iface.get("businfo", "")

            if not mac:
                continue

            # Skip InfiniBand interfaces — GUIDs are longer than Ethernet MACs
            # Ethernet MACs: 17 chars (xx:xx:xx:xx:xx:xx)
            # IB GUIDs: 20+ bytes, often with extra octets
            if len(mac) > 17:
                logger.debug("Skipping InfiniBand interface: %s (%s)", iface.get("name", ""), mac[:20])
                continue

            # Skip GPU cards that lshw classifies as network
            vendor_lower = vendor.lower()
            if any(gv in vendor_lower for gv in self._GPU_AS_NIC_VENDORS):
                logger.debug("Skipping GPU-as-NIC: %s %s", vendor, product)
                continue

            # Group by PCI card — strip function number to get card identity
            # businfo format: "pci@0000:81:00.0" → card key "pci@0000:81:00"
            card_key = businfo.rsplit(".", 1)[0] if businfo and "." in businfo else mac
            if card_key in seen_pci_cards:
                continue
            seen_pci_cards.add(card_key)

            items.append({
                "product": product,
                "vendor": vendor,
                "serial": mac,  # First port's MAC as serial proxy
                "description": iface.get("description", ""),
                "name": iface.get("name", ""),
            })

        return items

    def _get_local_psus(self):
        """Detect PSUs via dmidecode type 39."""
        items = []
        try:
            from netbox_agent import dmidecode
            dmi = self.server.dmi
            # Use numeric type ID 39 because _str2type has " Power Supply"
            # (with leading space), causing string lookup to fail.
            psus = dmidecode.get_by_type(dmi, 39) or []
            for psu in psus:
                name = psu.get("Name", "Unknown PSU")
                serial = psu.get("Serial Number", "")
                manufacturer = psu.get("Manufacturer", "Unknown")
                if serial in ("", "Not Specified", "To Be Filled By O.E.M.", "N/A", "NULL"):
                    serial = None
                if name in ("Not Specified", "To Be Filled By O.E.M."):
                    name = "Unknown PSU"
                # Skip PSUs with no usable data (BMC doesn't expose FRU details)
                max_power = psu.get("Max Power Capacity", "0 W")
                if manufacturer in ("NULL", "Unknown", "") and serial is None and (
                    "0 W" in str(max_power) or max_power in ("Unknown", "")
                ):
                    logger.debug("Skipping PSU with no data: %s (manufacturer=%s, max_power=%s)",
                                 psu.get("Name", "?"), manufacturer, max_power)
                    continue
                items.append({
                    "product": f"{manufacturer} {name}".strip(),
                    "vendor": manufacturer,
                    "serial": serial,
                    "description": "Power Supply",
                })
        except Exception as e:
            logger.warning("PSU detection failed: %s", e)

        return items

    # Vendor keywords → canonical name (checked in order, first match wins)
    _VENDOR_KEYWORDS = (
        ("solidigm", "Solidigm"),
        ("samsung", "Samsung"),
        ("intel", "Intel"),
        ("micron", "Micron"),
        ("western digital", "Western Digital"),
        ("seagate", "Seagate"),
        ("toshiba", "Toshiba"),
        ("kioxia", "Kioxia"),
        ("hynix", "SK Hynix"),
        ("kingston", "Kingston"),
        ("crucial", "Crucial"),
        ("sandisk", "SanDisk"),
        ("hgst", "HGST"),
        ("hitachi", "Hitachi"),
        ("liteon", "Lite-On"),
        ("phison", "Phison"),
    )

    # Model number prefixes → vendor (for models that don't contain the vendor name)
    _VENDOR_PREFIXES = (
        ("st", "Seagate"),       # ST4000NM000A, ST8000NM000A
        ("wdc ", "Western Digital"),  # WDC WD4003FFBX
        ("wdc_", "Western Digital"),
        ("wd", "Western Digital"),   # WD4003FFBX
    )

    def _guess_vendor(self, product):
        """Guess vendor from product/model name keywords and prefixes."""
        product_lower = product.lower()
        for keyword, name in self._VENDOR_KEYWORDS:
            if keyword in product_lower:
                return name
        # Check model number prefixes
        for prefix, name in self._VENDOR_PREFIXES:
            if product_lower.startswith(prefix):
                return name
        return None

    # ------------------------------------------------------------------ #
    #  Module Type Resolution
    # ------------------------------------------------------------------ #

    def _get_profile(self, profile_name):
        """Get a module type profile by name (cached)."""
        if profile_name in self._profile_cache:
            return self._profile_cache[profile_name]
        profile = _api_retry(nb.dcim.module_type_profiles.get, name=profile_name)
        if profile:
            self._profile_cache[profile_name] = profile
        return profile

    def _get_or_create_manufacturer(self, name):
        """Find or create a manufacturer (cached)."""
        if not name or name in ("Unknown", "N/A"):
            name = "Unknown"
        if name in self._manufacturer_cache:
            return self._manufacturer_cache[name]

        mfr = _api_retry(nb.dcim.manufacturers.get, name=name)
        if not mfr:
            slug = re.sub(r"[^A-Za-z0-9]+", "-", name).lower().strip("-")
            if not slug:
                slug = "unknown"
            # Slug-based fallback (handles casing differences like "Broadcom" vs "BROADCOM")
            mfr = _api_retry(nb.dcim.manufacturers.get, slug=slug)
            if not mfr:
                mfr = _api_retry(nb.dcim.manufacturers.create, name=name, slug=slug)
                logger.info("Created manufacturer '%s'", name)
        self._manufacturer_cache[name] = mfr
        return mfr

    def _resolve_module_type(self, category, item):
        """
        Find or auto-create a ModuleType for the given hardware item.

        Args:
            category: One of 'cpu', 'gpu', 'dimm', 'ssd', 'nic', 'psu'
            item: dict with at least 'product' and 'vendor'

        Returns:
            pynetbox ModuleType record
        """
        product = item["product"]
        vendor = item.get("vendor", "Unknown")
        cache_key = f"{vendor}::{product}"

        if cache_key in self._module_type_cache:
            return self._module_type_cache[cache_key]

        mfr = self._get_or_create_manufacturer(vendor)
        profile_name = CATEGORIES[category]["profile"]
        profile = self._get_profile(profile_name)

        # Try to find existing module type
        mt = _api_retry(nb.dcim.module_types.get, manufacturer_id=mfr.id, model=product)
        if mt:
            self._module_type_cache[cache_key] = mt
            return mt

        # Auto-create with profile (no attribute_data — admin fills in later)
        create_params = {
            "manufacturer": mfr.id,
            "model": product,
        }
        if profile:
            create_params["profile"] = profile.id

        mt = _api_retry(nb.dcim.module_types.create, create_params)
        logger.info("Auto-created module type '%s / %s' (profile=%s)", vendor, product, profile_name)
        self._module_type_cache[cache_key] = mt
        return mt

    def _default_module_custom_fields(self):
        """Return custom_fields dict for new module creation."""
        return {
            "owner": self.default_owner,
            "record_completeness": "incomplete",
        }

    # ------------------------------------------------------------------ #
    #  Module Bay Management
    # ------------------------------------------------------------------ #

    def _ensure_module_bays(self, device, category, count):
        """
        Ensure the device has at least `count` module bays for `category`.
        Creates any missing bays.

        Returns:
            list of module bay records sorted by name
        """
        prefix = CATEGORIES[category]["prefix"]
        existing_bays = list(_api_retry(nb.dcim.module_bays.filter, device_id=device.id))
        category_bays = [b for b in existing_bays if b.name.startswith(f"{prefix}-")]
        existing_names = {b.name for b in category_bays}

        for i in range(count):
            bay_name = f"{prefix}-{i}"
            if bay_name not in existing_names:
                _api_retry(nb.dcim.module_bays.create, {
                    "device": device.id,
                    "name": bay_name,
                    "position": bay_name,
                })
                logger.info("Created module bay '%s' on device '%s'", bay_name, device.name)

        # Re-fetch to get the complete list
        all_bays = list(_api_retry(nb.dcim.module_bays.filter, device_id=device.id))
        category_bays = sorted(
            [b for b in all_bays if b.name.startswith(f"{prefix}-")],
            key=lambda b: (b.name.rsplit("-", 1)[0], int(b.name.rsplit("-", 1)[1]) if b.name.rsplit("-", 1)[-1].isdigit() else 0),
        )
        return category_bays

    def _prune_and_renumber_bays(self, device, category, detected_count):
        """
        Remove excess empty bays and renumber remaining bays sequentially.

        If the device has more bays than detected items for a category,
        the extras are orphans from a prior detection that no longer
        matches reality (drive removed, transient detection, etc.).

        After pruning, renumber bays to close gaps:
        SSD-0, SSD-1, ..., SSD-N with no missing indices.
        """
        prefix = CATEGORIES[category]["prefix"]
        all_bays = list(_api_retry(nb.dcim.module_bays.filter, device_id=device.id))
        category_bays = sorted(
            [b for b in all_bays if b.name.startswith(f"{prefix}-")],
            key=lambda b: (b.name.rsplit("-", 1)[0], int(b.name.rsplit("-", 1)[1]) if b.name.rsplit("-", 1)[-1].isdigit() else 0),
        )

        if len(category_bays) <= detected_count:
            return  # Nothing to prune

        # Separate populated and empty bays
        populated_bays = []
        empty_bays = []
        for bay in category_bays:
            if bay.installed_module:
                populated_bays.append(bay)
            else:
                empty_bays.append(bay)

        excess = len(category_bays) - detected_count
        to_delete = empty_bays[:excess]

        for bay in to_delete:
            logger.info(
                "Pruning orphan bay '%s' on '%s' (detected %d, had %d bays)",
                bay.name, device.name, detected_count, len(category_bays),
            )
            _api_retry(bay.delete)

        if not to_delete:
            return  # Nothing was pruned, skip renumber

        # Renumber remaining bays sequentially
        remaining_bays = list(_api_retry(nb.dcim.module_bays.filter, device_id=device.id))
        remaining_category = sorted(
            [b for b in remaining_bays if b.name.startswith(f"{prefix}-")],
            key=lambda b: (b.name.rsplit("-", 1)[0], int(b.name.rsplit("-", 1)[1]) if b.name.rsplit("-", 1)[-1].isdigit() else 0),
        )

        for i, bay in enumerate(remaining_category):
            expected_name = f"{prefix}-{i}"
            if bay.name != expected_name:
                logger.info(
                    "Renumbering bay '%s' → '%s' on '%s'",
                    bay.name, expected_name, device.name,
                )
                bay.name = expected_name
                bay.position = expected_name
                _api_retry(bay.save)

    def _get_device_modules(self, device, category):
        """
        Get all modules installed on a device in a given category.

        Returns:
            list of module records
        """
        prefix = CATEGORIES[category]["prefix"]
        all_modules = list(_api_retry(nb.dcim.modules.filter, device_id=device.id))
        # Filter by bay name prefix
        category_modules = []
        for mod in all_modules:
            bay = mod.module_bay
            if bay and hasattr(bay, "name") and bay.name.startswith(f"{prefix}-"):
                category_modules.append(mod)
            elif bay and hasattr(bay, "display") and bay.display.startswith(f"{prefix}-"):
                category_modules.append(mod)
        return category_modules

    # ------------------------------------------------------------------ #
    #  Re-parenting Logic
    # ------------------------------------------------------------------ #

    def _get_spare_device(self):
        """Get (cached) the SPARE-INVENTORY device."""
        if self._spare_device is not None:
            return self._spare_device
        spare_name = getattr(self.config, "spare_device_name", "SPARE-INVENTORY")
        self._spare_device = _api_retry(nb.dcim.devices.get, name=spare_name)
        if not self._spare_device:
            logger.error("Spare device '%s' not found in NetBox", spare_name)
        return self._spare_device

    def _find_module_by_serial(self, serial):
        """Search all modules by serial number. Returns first match or None."""
        if not serial:
            return None
        results = list(_api_retry(nb.dcim.modules.filter, serial=serial))
        if len(results) > 1:
            logger.warning("Duplicate serial '%s' found on %d modules — using first match", serial, len(results))
        return results[0] if results else None

    def _reparent_module(self, module, target_device, target_bay):
        """Move a module to a different device and bay."""
        logger.info(
            "Re-parenting module '%s' (serial=%s) → device '%s' bay '%s'",
            module, module.serial, target_device.name, target_bay.name,
        )
        module.device = target_device.id
        module.module_bay = target_bay.id
        _api_retry(module.save)

    def _move_to_spare(self, module, category):
        """Move a module to the SPARE-INVENTORY device."""
        spare = self._get_spare_device()
        if not spare:
            logger.error("Cannot move module to spare — spare device not found")
            return False

        prefix = CATEGORIES[category]["prefix"]
        spare_bays = list(_api_retry(nb.dcim.module_bays.filter, device_id=spare.id))
        spare_category_bays = sorted(
            [b for b in spare_bays if b.name.startswith(f"{prefix}-")],
            key=lambda b: (b.name.rsplit("-", 1)[0], int(b.name.rsplit("-", 1)[1]) if b.name.rsplit("-", 1)[-1].isdigit() else 0),
        )

        # Find an unoccupied bay
        spare_modules = list(_api_retry(nb.dcim.modules.filter, device_id=spare.id))
        occupied_bay_ids = set()
        for m in spare_modules:
            if m.module_bay:
                bay_id = m.module_bay.id if hasattr(m.module_bay, "id") else m.module_bay
                occupied_bay_ids.add(bay_id)

        target_bay = None
        for bay in spare_category_bays:
            if bay.id not in occupied_bay_ids:
                target_bay = bay
                break

        if not target_bay:
            logger.error(
                "No free %s bay on spare device — admin must expand spare bays", prefix
            )
            return False

        self._reparent_module(module, spare, target_bay)
        return True

    def _vacate_bay(self, bay, category):
        """If a bay is occupied, move its occupant to spare."""
        modules_in_bay = list(_api_retry(nb.dcim.modules.filter, module_bay_id=bay.id))
        for mod in modules_in_bay:
            logger.info("Bay '%s' occupied by module (serial=%s) — moving to spare", bay.name, mod.serial)
            self._move_to_spare(mod, category)

    # ------------------------------------------------------------------ #
    #  Core Sync Algorithm
    # ------------------------------------------------------------------ #

    def _sync_category(self, category, local_items):
        """
        Sync a single hardware category.

        Algorithm (serial-first, stable bay assignment):
        1. Ensure device has enough module bays
        2. Pass 1 — match existing: for each detected item with a serial
           already on this device, mark it matched and leave its bay alone.
           Only update module_type if it changed.
        3. Pass 2 — place new/remote: for detected items NOT matched in
           pass 1, find an empty bay and either re-parent from another
           device or create new.
        4. Move unmatched existing modules to spare (hardware removed).

        Key invariant: modules already on the correct device are NEVER
        moved between bays. Detection order may vary between boots
        (NVMe enumeration, PCI probe order) but bay assignments are
        stable once set.
        """
        if not local_items:
            # Move all existing modules in this category to spare
            existing = self._get_device_modules(self.device, category)
            for mod in existing:
                self._move_to_spare(mod, category)
            return

        prefix = CATEGORIES[category]["prefix"]

        # Step 1: Ensure enough bays
        bays = self._ensure_module_bays(self.device, category, len(local_items))

        # Step 2: Get existing modules on this device for this category
        existing_modules = self._get_device_modules(self.device, category)
        existing_by_serial = {}
        for mod in existing_modules:
            if mod.serial:
                existing_by_serial[mod.serial] = mod

        # Track which bays are occupied and which modules are matched
        matched_module_ids = set()
        occupied_bay_ids = set()
        for mod in existing_modules:
            if mod.module_bay:
                bay_id = mod.module_bay.id if hasattr(mod.module_bay, "id") else mod.module_bay
                occupied_bay_ids.add(bay_id)

        has_serial = any(item.get("serial") for item in local_items)

        # Items that need placement (not already on this device)
        needs_placement = []

        # --- Pass 1: match serials already on this device (stable bays) ---
        for item in local_items:
            serial = item.get("serial")
            if not serial:
                needs_placement.append(item)
                continue

            if serial in existing_by_serial:
                mod = existing_by_serial[serial]
                matched_module_ids.add(mod.id)

                # Update module type if changed — but never move bays
                module_type = self._resolve_module_type(category, item)
                mod_mt_id = None
                if mod.module_type:
                    mod_mt_id = mod.module_type.id if hasattr(mod.module_type, "id") else mod.module_type
                if mod_mt_id != module_type.id:
                    mod.module_type = module_type.id
                    _api_retry(mod.save)
                    logger.info("Updated module type serial=%s on %s", serial, self.device.name)
            else:
                needs_placement.append(item)

        # --- Pass 2: place new/remote items into empty bays ---
        # Build list of empty bays (not occupied by any module)
        empty_bays = [b for b in bays if b.id not in occupied_bay_ids]
        _serialless_adopted_bays = set()  # Track bays adopted by serialless items

        for item in needs_placement:
            serial = item.get("serial")
            module_type = self._resolve_module_type(category, item)

            if serial:
                # Check if exists on another device (hardware moved here)
                remote_mod = self._find_module_by_serial(serial)
                if remote_mod:
                    if not empty_bays:
                        logger.warning(
                            "No empty bay for serial=%s on %s — skipping",
                            serial, self.device.name,
                        )
                        continue
                    target_bay = empty_bays.pop(0)
                    self._reparent_module(remote_mod, self.device, target_bay)
                    matched_module_ids.add(remote_mod.id)
                    occupied_bay_ids.add(target_bay.id)

                    # Update module type if changed
                    mod_mt_id = None
                    if remote_mod.module_type:
                        mod_mt_id = remote_mod.module_type.id if hasattr(remote_mod.module_type, "id") else remote_mod.module_type
                    if mod_mt_id != module_type.id:
                        remote_mod.module_type = module_type.id
                        _api_retry(remote_mod.save)
                    continue

                # Not found anywhere — create new
                if not empty_bays:
                    logger.warning(
                        "No empty bay for new serial=%s on %s — skipping",
                        serial, self.device.name,
                    )
                    continue
                target_bay = empty_bays.pop(0)
                new_mod = _api_retry(nb.dcim.modules.create, {
                    "device": self.device.id,
                    "module_bay": target_bay.id,
                    "module_type": module_type.id,
                    "serial": serial,
                    "status": "active",
                    "custom_fields": self._default_module_custom_fields(),
                })
                matched_module_ids.add(new_mod.id)
                occupied_bay_ids.add(target_bay.id)
                logger.info(
                    "Created module %s serial=%s on %s bay=%s",
                    item["product"], serial, self.device.name, target_bay.name,
                )

            else:
                # --- No serial (e.g., CPUs, GPUs): positional matching ---
                # Find a bay that either has a module we can adopt or is empty.
                # First try to adopt existing modules in order, then create in empty bays.
                target_bay = None

                # Try occupied bays first (adopt existing modules)
                for bay in bays:
                    if bay.id in occupied_bay_ids and bay.id not in _serialless_adopted_bays:
                        modules_in_bay = list(_api_retry(nb.dcim.modules.filter, module_bay_id=bay.id))
                        if modules_in_bay:
                            mod = modules_in_bay[0]
                            matched_module_ids.add(mod.id)
                            _serialless_adopted_bays.add(bay.id)
                            # Update module type if changed
                            mod_mt_id = None
                            if mod.module_type:
                                mod_mt_id = mod.module_type.id if hasattr(mod.module_type, "id") else mod.module_type
                            if mod_mt_id != module_type.id:
                                mod.module_type = module_type.id
                                _api_retry(mod.save)
                                logger.info("Updated module type at %s on %s", bay.name, self.device.name)
                            target_bay = bay
                            break

                if target_bay:
                    continue

                # No existing module to adopt — create in an empty bay
                if empty_bays:
                    target_bay = empty_bays.pop(0)
                    new_mod = _api_retry(nb.dcim.modules.create, {
                        "device": self.device.id,
                        "module_bay": target_bay.id,
                        "module_type": module_type.id,
                        "status": "active",
                        "custom_fields": self._default_module_custom_fields(),
                    })
                    matched_module_ids.add(new_mod.id)
                    occupied_bay_ids.add(target_bay.id)
                    _serialless_adopted_bays.add(target_bay.id)
                    logger.info(
                        "Created module %s (no serial) on %s bay=%s",
                        item["product"], self.device.name, target_bay.name,
                    )
                else:
                    logger.warning(
                        "No bay available for serialless %s on %s — skipping",
                        item.get("product", "?"), self.device.name,
                    )

        # Step 3: Move unmatched existing modules to spare (hardware removed)
        if has_serial:
            for mod in existing_modules:
                if mod.id not in matched_module_ids:
                    logger.info(
                        "Module serial=%s no longer detected on %s — moving to spare",
                        mod.serial, self.device.name,
                    )
                    self._move_to_spare(mod, category)

        # Step 4: Prune orphan empty bays and renumber sequentially
        self._prune_and_renumber_bays(self.device, category, len(local_items))

    # ------------------------------------------------------------------ #
    #  Public Interface
    # ------------------------------------------------------------------ #

    def create_or_update(self, deps=None, state=None):
        """
        Main entry point: detect local hardware and sync all categories to NetBox.
        Must be called after the device exists in NetBox.

        Args:
            deps: dict of {tool_name: bool} from dependencies.check_all()
            state: StateManager instance for diff-based sync (optional)
        """
        self.device = self.server.get_netbox_server()
        if not self.device:
            logger.error("Device not found in NetBox — cannot sync modules")
            return False

        logger.info("Starting module sync for device '%s' (id=%d)", self.device.name, self.device.id)

        # Skip PSU detection when dmidecode is unavailable
        skip_psu = deps is not None and not deps.get("dmidecode", True)

        # Detect all local hardware
        detections = {
            "cpu": self._get_local_cpus(),
            "gpu": self._get_local_gpus(),
            "accelerator": self._get_local_accelerators(),
            "dimm": self._get_local_dimms(),
            "ssd": self._get_local_ssds(),
            "nic": self._get_local_nics(),
        }
        if skip_psu:
            logger.info("Skipping PSU detection — dmidecode unavailable")
            detections["psu"] = []
        else:
            detections["psu"] = self._get_local_psus()

        for category, items in detections.items():
            logger.info("Detected %d %s(s)", len(items), category)

            # Diff-based sync: skip unchanged categories
            if state is not None:
                changed, summary = state.diff_hardware(category, items)
                if not changed:
                    logger.info("Skipping %s sync — unchanged", category)
                    continue
                else:
                    logger.info("Hardware changed for %s: %s", category, summary)

            try:
                self._sync_category(category, items)
            except Exception as e:
                logger.error("Failed to sync %s: %s", category, e)
                # Continue with other categories

        # Save state after successful sync
        if state is not None:
            try:
                hostname = self.device.name
                # Convert items to serializable format
                hw_state = {}
                for cat, items in detections.items():
                    hw_state[cat] = [
                        {k: v for k, v in item.items() if k in ("product", "vendor", "serial")}
                        for item in items
                    ]
                state.save(hostname, hw_state, dependencies=deps)
            except Exception as e:
                logger.warning("Failed to save state: %s", e)

        logger.info("Module sync complete for device '%s'", self.device.name)
        return True

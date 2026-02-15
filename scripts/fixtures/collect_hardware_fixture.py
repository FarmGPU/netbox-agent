#!/usr/bin/env python3
"""
Hardware Fixture Collector — captures raw hardware data from a real server.

Designed to be self-contained (no netbox-agent dependencies) so it can run on
any server. Outputs a single JSON fixture file that the integration tests and
agent parsers can consume.

Usage:
    sudo python3 collect_hardware_fixture.py [--output /path/to/fixture.json]

Requires root for dmidecode and ipmitool. Non-root runs will skip those tools.
"""

import argparse
import json
import logging
import os
import platform
import shutil
import subprocess
import sys
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def _run(cmd, timeout=60):
    """Run a command and return (stdout, stderr, returncode)."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.stdout, result.stderr, result.returncode
    except FileNotFoundError:
        return "", f"command not found: {cmd[0]}", 127
    except subprocess.TimeoutExpired:
        return "", f"timeout after {timeout}s", 124
    except Exception as e:
        return "", str(e), 1


def _run_json(cmd, timeout=60):
    """Run a command and parse stdout as JSON. Returns (parsed_data, raw_stdout, error)."""
    stdout, stderr, rc = _run(cmd, timeout)
    if rc != 0:
        return None, stdout, stderr
    try:
        data = json.loads(stdout)
        return data, stdout, None
    except (json.JSONDecodeError, ValueError) as e:
        return None, stdout, f"JSON parse error: {e}"


def _has_tool(name):
    """Check if a command is available on PATH."""
    return shutil.which(name) is not None


def collect_lshw():
    """Collect lshw -quiet -json output."""
    if not _has_tool("lshw"):
        logger.warning("lshw not found — skipping")
        return {"available": False, "error": "lshw not installed"}

    data, raw, err = _run_json(["lshw", "-quiet", "-json"])
    if err:
        logger.warning("lshw failed: %s", err)
        return {"available": True, "error": err, "raw": raw[:5000]}
    return {"available": True, "data": data}


def collect_lsblk():
    """Collect lsblk -J -b with storage-relevant columns."""
    if not _has_tool("lsblk"):
        logger.warning("lsblk not found — skipping")
        return {"available": False, "error": "lsblk not installed"}

    columns = "NAME,TYPE,SIZE,MODEL,SERIAL,VENDOR,TRAN,ROTA,HCTL,SUBSYSTEMS,REV,MOUNTPOINT,FSTYPE,MAJ:MIN"
    data, raw, err = _run_json(["lsblk", "-J", "-b", "-o", columns])
    if err:
        # Some kernels don't support all columns — try minimal set
        logger.warning("lsblk full columns failed (%s), trying minimal...", err)
        data, raw, err = _run_json(["lsblk", "-J", "-b", "-o", "NAME,TYPE,SIZE,MODEL,SERIAL,VENDOR,TRAN,ROTA"])
        if err:
            return {"available": True, "error": err, "raw": raw[:5000]}
    return {"available": True, "data": data}


def collect_nvme_list():
    """Collect nvme list -o json output."""
    if not _has_tool("nvme"):
        logger.warning("nvme-cli not found — skipping")
        return {"available": False, "error": "nvme-cli not installed"}

    data, raw, err = _run_json(["nvme", "list", "-o", "json"])
    if err:
        logger.warning("nvme list failed: %s", err)
        return {"available": True, "error": err, "raw": raw[:5000]}
    return {"available": True, "data": data}


def collect_dmidecode():
    """Collect full dmidecode output as JSON (type-indexed)."""
    if not _has_tool("dmidecode"):
        logger.warning("dmidecode not found — skipping")
        return {"available": False, "error": "dmidecode not installed"}

    stdout, stderr, rc = _run(["dmidecode"])
    if rc != 0:
        return {"available": True, "error": stderr, "raw": stdout[:5000]}

    # Also collect specific types we care about
    types = {
        "bios": 0,
        "system": 1,
        "baseboard": 2,
        "chassis": 3,
        "processor": 4,
        "memory_controller": 5,
        "memory_module": 6,
        "memory_device": 17,
        "power_supply": 39,
    }

    type_data = {}
    for name, type_id in types.items():
        t_stdout, t_stderr, t_rc = _run(["dmidecode", "-t", str(type_id)])
        type_data[name] = {
            "type_id": type_id,
            "output": t_stdout if t_rc == 0 else None,
            "error": t_stderr if t_rc != 0 else None,
        }

    return {
        "available": True,
        "full_output": stdout,
        "by_type": type_data,
    }


def collect_nvidia_smi():
    """Collect nvidia-smi GPU data."""
    if not _has_tool("nvidia-smi"):
        logger.info("nvidia-smi not found — no NVIDIA GPUs or driver not installed")
        return {"available": False, "error": "nvidia-smi not installed"}

    results = {}

    # Query GPU details
    query_fields = "index,name,serial,uuid,pci.bus_id,memory.total,driver_version,power.limit"
    stdout, stderr, rc = _run([
        "nvidia-smi",
        f"--query-gpu={query_fields}",
        "--format=csv,noheader",
    ])
    if rc == 0:
        results["query_csv"] = stdout
    else:
        results["query_error"] = stderr

    # Also try JSON-like XML output
    stdout_xml, stderr_xml, rc_xml = _run(["nvidia-smi", "-q", "-x"])
    if rc_xml == 0:
        results["xml"] = stdout_xml
    else:
        results["xml_error"] = stderr_xml

    results["available"] = True
    return results


def collect_lspci():
    """Collect lspci -vmm (machine-readable) output."""
    if not _has_tool("lspci"):
        logger.warning("lspci not found — skipping")
        return {"available": False, "error": "lspci not installed"}

    stdout, stderr, rc = _run(["lspci", "-vmm"])
    if rc != 0:
        return {"available": True, "error": stderr}

    # Also collect numeric IDs version for precise matching
    stdout_nn, _, rc_nn = _run(["lspci", "-nn"])

    return {
        "available": True,
        "vmm": stdout,
        "nn": stdout_nn if rc_nn == 0 else None,
    }


def collect_ipmitool():
    """Collect ipmitool FRU and MC info."""
    if not _has_tool("ipmitool"):
        logger.warning("ipmitool not found — skipping")
        return {"available": False, "error": "ipmitool not installed"}

    results = {"available": True}

    # FRU data
    stdout, stderr, rc = _run(["ipmitool", "fru", "print"])
    results["fru"] = stdout if rc == 0 else None
    if rc != 0:
        results["fru_error"] = stderr

    # MC info (BMC firmware version etc.)
    stdout, stderr, rc = _run(["ipmitool", "mc", "info"])
    results["mc_info"] = stdout if rc == 0 else None

    # LAN info for BMC MAC
    stdout, stderr, rc = _run(["ipmitool", "lan", "print", "1"])
    results["lan"] = stdout if rc == 0 else None

    return results


def collect_lscpu():
    """Collect lscpu -J (JSON) output, informed by SILO's cpu.py parser."""
    if not _has_tool("lscpu"):
        logger.warning("lscpu not found — skipping")
        return {"available": False, "error": "lscpu not installed"}

    data, raw, err = _run_json(["lscpu", "-J"])
    if err:
        logger.warning("lscpu -J failed: %s", err)
        # Fallback to plain lscpu
        stdout, stderr, rc = _run(["lscpu"])
        return {"available": True, "error": err, "plain": stdout}
    return {"available": True, "data": data}


def collect_system_info():
    """Collect basic system identification."""
    info = {
        "hostname": platform.node(),
        "kernel": platform.release(),
        "arch": platform.machine(),
        "distro": "",
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "collected_by": "collect_hardware_fixture.py",
        "uid": os.getuid(),
    }

    # Try to read /etc/os-release
    try:
        with open("/etc/os-release") as f:
            for line in f:
                if line.startswith("PRETTY_NAME="):
                    info["distro"] = line.split("=", 1)[1].strip().strip('"')
                    break
    except FileNotFoundError:
        pass

    return info


def main():
    parser = argparse.ArgumentParser(
        description="Collect hardware fixture data from a real server"
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Output file path (default: <hostname>_fixture.json)",
    )
    parser.add_argument(
        "--skip",
        nargs="*",
        default=[],
        choices=["lshw", "lsblk", "nvme", "dmidecode", "nvidia", "lspci", "ipmitool", "lscpu"],
        help="Skip specific collectors",
    )
    args = parser.parse_args()

    hostname = platform.node()
    output_path = args.output or f"{hostname}_fixture.json"
    skip = set(args.skip)

    logger.info("Collecting hardware fixture for %s", hostname)

    fixture = {
        "system": collect_system_info(),
    }

    collectors = {
        "lshw": collect_lshw,
        "lsblk": collect_lsblk,
        "nvme": collect_nvme_list,
        "dmidecode": collect_dmidecode,
        "nvidia": collect_nvidia_smi,
        "lspci": collect_lspci,
        "ipmitool": collect_ipmitool,
        "lscpu": collect_lscpu,
    }

    for name, collector in collectors.items():
        if name in skip:
            logger.info("Skipping %s (--skip)", name)
            fixture[name] = {"available": False, "error": "skipped by user"}
            continue
        logger.info("Collecting %s...", name)
        try:
            fixture[name] = collector()
        except Exception as e:
            logger.error("Collector %s crashed: %s", name, e)
            fixture[name] = {"available": False, "error": str(e)}

    # Write fixture
    with open(output_path, "w") as f:
        json.dump(fixture, f, indent=2, default=str)

    size_kb = os.path.getsize(output_path) / 1024
    logger.info("Fixture written to %s (%.1f KB)", output_path, size_kb)
    logger.info("Transfer this file to tests/fixtures/ in the netbox-agent repo")

    # Print summary
    print(f"\n{'='*60}")
    print(f"Fixture Summary: {hostname}")
    print(f"{'='*60}")
    for name, data in fixture.items():
        if name == "system":
            continue
        available = data.get("available", False)
        status = "✓" if available else "✗"
        detail = ""
        if not available:
            detail = f" — {data.get('error', 'unknown')}"
        elif "data" in data:
            # Count items if possible
            d = data["data"]
            if isinstance(d, dict):
                if "blockdevices" in d:
                    detail = f" — {len(d['blockdevices'])} block devices"
                elif "Devices" in d:
                    detail = f" — {len(d['Devices'])} NVMe devices"
                elif "lscpu" in d:
                    detail = f" — {len(d['lscpu'])} fields"
        print(f"  {status} {name}{detail}")
    print(f"{'='*60}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

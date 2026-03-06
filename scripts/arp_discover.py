#!/usr/bin/env python3
"""ARP-based host discovery for initial NetBox population.

Discovers MAC→IP pairs on management subnets by SSH-ing into a pivot host
on each subnet, running a ping sweep + ARP cache dump, then cross-referencing
results with NetBox to identify known and unknown devices.

Usage:
    # Discover all configured subnets
    python3 scripts/arp_discover.py

    # Discover specific subnet
    python3 scripts/arp_discover.py --subnet 10.100.200.0/24

    # Output CSV for further processing
    python3 scripts/arp_discover.py --csv > discovery.csv

    # Also try SSH into discovered IPs to get hostname/serial
    python3 scripts/arp_discover.py --identify

Requirements:
    - SSH access (key-based) to at least one host per target subnet
    - pynetbox (for NetBox cross-referencing)
    - No special tools needed on pivot hosts (uses ping + ip neigh)
"""

import argparse
import csv
import ipaddress
import json
import logging
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Subnet → pivot host mapping
# Each subnet needs a host we can SSH into to perform the ARP scan
# ---------------------------------------------------------------------------
SUBNETS = {
    "10.100.200.0/24": {
        "pivot": "10.100.200.44",       # hickory09
        "name": "mgmt-200 (storage/compute)",
    },
    "10.100.208.0/24": {
        "pivot": "10.100.208.40",       # anaheim14
        "name": "mgmt-208 (GPU servers)",
    },
    "10.100.10.0/24": {
        "pivot": "10.100.10.56",        # tyan-milan-1
        "name": "mgmt-10 (infrastructure)",
    },
    "192.168.211.0/24": {
        "pivot": "192.168.211.36",      # bell01 (via jump host)
        "name": "mgmt-211 (legacy GPU)",
        "jump": "fgpu@10.100.191.46",
    },
}

# SSH configuration
SSH_KEYS = [
    os.path.expanduser("~/.ssh/fgpu"),
    os.path.expanduser("~/.ssh/og-fgpu"),
]
SSH_USER = "fgpu"
SSH_OPTS = [
    "-o", "BatchMode=yes",
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "ConnectTimeout=10",
    "-o", "LogLevel=ERROR",
]

# NetBox
NETBOX_URL = os.environ.get("NETBOX_URL", "https://10.100.248.18")
NETBOX_TOKEN = os.environ.get("NETBOX_TOKEN", "")


def _ssh_key_args():
    """Return SSH -i flags for available keys."""
    args = []
    for k in SSH_KEYS:
        if os.path.exists(k):
            args.extend(["-i", k])
    return args


def _ssh_cmd(host, command, jump=None):
    """Build SSH command for remote execution."""
    cmd = ["ssh"] + SSH_OPTS + _ssh_key_args()
    if jump:
        # Use ProxyCommand instead of -J to ensure keys are passed correctly
        key_args = " ".join(f"-i {k}" for k in SSH_KEYS if os.path.exists(k))
        proxy = (
            f"ssh -W %h:%p -o StrictHostKeyChecking=no "
            f"-o UserKnownHostsFile=/dev/null {key_args} {jump}"
        )
        cmd.extend(["-o", f"ProxyCommand={proxy}"])
    cmd.append(f"{SSH_USER}@{host}")
    cmd.append(command)
    return cmd


def arp_scan_subnet(subnet_cidr, pivot_host, jump=None):
    """SSH into pivot host, ping-sweep the subnet, return MAC→IP pairs.

    Uses parallel ping + 'ip neigh' to discover hosts via ARP.
    No special tools needed on the pivot — just ping and ip.
    """
    net = ipaddress.ip_network(subnet_cidr, strict=False)

    # Build a remote script that:
    # 1. Pings all IPs in the subnet concurrently (background jobs)
    # 2. Waits for all pings to complete
    # 3. Dumps the ARP cache
    # We batch pings to avoid overwhelming the shell
    remote_script = f"""
set +e
# Ping sweep — background all pings, batched
for i in $(seq 1 254); do
    ip="{net.network_address + 0}"
    host=$(echo $ip | sed "s/\\.0$/.$i/")
    ping -c1 -W1 $host >/dev/null 2>&1 &
    # Batch: wait every 50 pings to avoid too many background jobs
    if [ $((i % 50)) -eq 0 ]; then wait; fi
done
wait
# Dump ARP cache — only REACHABLE/STALE/DELAY entries with MAC
ip neigh show | grep -v FAILED | grep -v INCOMPLETE | awk '{{print $1, $5}}'
"""

    logger.info("Scanning %s via pivot %s ...", subnet_cidr, pivot_host)
    cmd = _ssh_cmd(pivot_host, remote_script, jump=jump)

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=120
        )
        if result.returncode != 0:
            logger.error("SSH to %s failed: %s", pivot_host, result.stderr.strip())
            return []
    except subprocess.TimeoutExpired:
        logger.error("Scan of %s timed out", subnet_cidr)
        return []

    # Parse output: "IP MAC" per line
    entries = []
    seen = set()
    for line in result.stdout.strip().split("\n"):
        parts = line.split()
        if len(parts) >= 2:
            ip, mac = parts[0], parts[1].upper()
            # Validate MAC format (6 octets)
            if len(mac.split(":")) == 6 and ip not in seen:
                try:
                    addr = ipaddress.ip_address(ip)
                    if addr in net:
                        entries.append({"ip": ip, "mac": mac})
                        seen.add(ip)
                except ValueError:
                    pass

    logger.info("  Found %d hosts on %s", len(entries), subnet_cidr)
    return entries


def load_netbox_data():
    """Load all device interfaces with MACs and IPs from NetBox."""
    try:
        import pynetbox
        import urllib3
        urllib3.disable_warnings()
    except ImportError:
        logger.warning("pynetbox not installed — skipping NetBox cross-reference")
        return None

    token = NETBOX_TOKEN
    if not token:
        # Try to read from deploy script
        deploy_script = os.path.join(
            os.path.dirname(__file__), "deploy_and_run.sh"
        )
        if os.path.exists(deploy_script):
            with open(deploy_script) as f:
                for line in f:
                    if line.startswith("NETBOX_TOKEN="):
                        token = line.split("=", 1)[1].strip().strip('"\'')
                        break

    if not token:
        logger.warning("No NetBox token — skipping cross-reference")
        return None

    nb = pynetbox.api(NETBOX_URL, token=token)
    nb.http_session.verify = False

    logger.info("Loading device data from NetBox ...")

    # Build MAC → device mapping from interfaces
    mac_to_device = {}  # MAC → {device_name, device_id, iface_name, primary_ip}
    device_info = {}    # device_id → {name, serial, primary_ip, bmc_mac}

    devices = list(nb.dcim.devices.filter(status="active", role_id=2))
    for dev in devices:
        cf = dev.custom_fields or {}
        primary = str(dev.primary_ip4).split("/")[0] if dev.primary_ip4 else None
        device_info[dev.id] = {
            "name": dev.name or "UNNAMED",
            "serial": dev.serial or "",
            "primary_ip": primary,
            "bmc_mac": cf.get("bmc_mac_address", ""),
            "modules": 0,  # filled later if needed
        }

        interfaces = list(nb.dcim.interfaces.filter(device_id=dev.id))
        for iface in interfaces:
            if iface.mac_address:
                mac = str(iface.mac_address).upper()
                mac_to_device[mac] = {
                    "device_name": dev.name or "UNNAMED",
                    "device_id": dev.id,
                    "iface_name": iface.name,
                    "primary_ip": primary,
                }

    logger.info("  Loaded %d devices, %d interface MACs", len(device_info), len(mac_to_device))
    return {"mac_to_device": mac_to_device, "device_info": device_info}


def identify_host(ip, jump=None):
    """Try to SSH into a discovered IP and get hostname + serial."""
    cmd = _ssh_cmd(ip, "hostname -f 2>/dev/null; sudo dmidecode -s system-serial-number 2>/dev/null || echo UNKNOWN", jump=jump)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            lines = result.stdout.strip().split("\n")
            hostname = lines[0] if lines else "?"
            serial = lines[1] if len(lines) > 1 else "?"
            return {"hostname": hostname, "serial": serial}
    except (subprocess.TimeoutExpired, Exception):
        pass
    return None


def main():
    parser = argparse.ArgumentParser(
        description="ARP-based host discovery for NetBox population"
    )
    parser.add_argument(
        "--subnet", "-s",
        help="Specific subnet to scan (e.g., 10.100.200.0/24). Default: all configured.",
    )
    parser.add_argument(
        "--csv", action="store_true",
        help="Output results as CSV",
    )
    parser.add_argument(
        "--identify", action="store_true",
        help="Try SSH into unknown hosts to get hostname/serial",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Output results as JSON",
    )
    args = parser.parse_args()

    # Select subnets to scan
    if args.subnet:
        if args.subnet not in SUBNETS:
            logger.error("Unknown subnet %s. Configured: %s", args.subnet, list(SUBNETS.keys()))
            sys.exit(1)
        targets = {args.subnet: SUBNETS[args.subnet]}
    else:
        targets = SUBNETS

    # Load NetBox data for cross-referencing
    nb_data = load_netbox_data()

    # Scan all subnets
    all_results = []
    for cidr, config in targets.items():
        entries = arp_scan_subnet(
            cidr,
            config["pivot"],
            jump=config.get("jump"),
        )

        for entry in entries:
            entry["subnet"] = cidr
            entry["subnet_name"] = config["name"]

            # Cross-reference with NetBox
            entry["nb_match"] = None
            entry["nb_device"] = None
            if nb_data:
                match = nb_data["mac_to_device"].get(entry["mac"])
                if match:
                    entry["nb_match"] = "interface_mac"
                    entry["nb_device"] = match["device_name"]
                    entry["nb_device_ip"] = match["primary_ip"]
                    entry["nb_iface"] = match["iface_name"]

            all_results.append(entry)

    # Try to identify unknown hosts via SSH
    if args.identify:
        unknowns = [r for r in all_results if not r["nb_match"]]
        if unknowns:
            logger.info("Trying SSH identification on %d unknown hosts ...", len(unknowns))
            # Determine jump host per subnet
            subnet_jumps = {c: cfg.get("jump") for c, cfg in SUBNETS.items()}
            with ThreadPoolExecutor(max_workers=10) as pool:
                futures = {}
                for r in unknowns:
                    jump = subnet_jumps.get(r["subnet"])
                    futures[pool.submit(identify_host, r["ip"], jump)] = r

                for future in as_completed(futures):
                    r = futures[future]
                    info = future.result()
                    if info:
                        r["ssh_hostname"] = info["hostname"]
                        r["ssh_serial"] = info["serial"]
                        # Try to match serial against NetBox
                        if nb_data and info["serial"] != "UNKNOWN":
                            for did, dinfo in nb_data["device_info"].items():
                                if dinfo["serial"] == info["serial"]:
                                    r["nb_match"] = "serial"
                                    r["nb_device"] = dinfo["name"]
                                    r["nb_device_ip"] = dinfo["primary_ip"]
                                    break

    # Output
    if args.json:
        print(json.dumps(all_results, indent=2))
        return

    if args.csv:
        writer = csv.DictWriter(
            sys.stdout,
            fieldnames=["subnet", "ip", "mac", "nb_match", "nb_device", "nb_device_ip",
                        "ssh_hostname", "ssh_serial"],
            extrasaction="ignore",
        )
        writer.writeheader()
        for r in sorted(all_results, key=lambda x: (x["subnet"], x["ip"])):
            writer.writerow(r)
        return

    # Human-readable table
    known = [r for r in all_results if r.get("nb_match")]
    unknown = [r for r in all_results if not r.get("nb_match")]

    print(f"\n{'='*110}")
    print(f"ARP Discovery Results — {len(all_results)} hosts found across {len(targets)} subnet(s)")
    print(f"{'='*110}")

    if known:
        print(f"\n--- KNOWN DEVICES ({len(known)}) — matched in NetBox ---")
        print(f"{'IP':<18} {'MAC':<19} {'Match':<10} {'NetBox Device':<45} {'NB IP'}")
        print("-" * 110)
        for r in sorted(known, key=lambda x: x["ip"]):
            nb_ip = r.get("nb_device_ip") or "NO IP"
            ip_status = ""
            if nb_ip == "NO IP":
                ip_status = " ← MISSING IN NB"
            elif nb_ip != r["ip"]:
                ip_status = f" ← MISMATCH (ARP={r['ip']})"
            print(f"  {r['ip']:<16} {r['mac']:<19} {r.get('nb_match',''):<10} "
                  f"{r.get('nb_device',''):<45} {nb_ip}{ip_status}")

    if unknown:
        print(f"\n--- UNKNOWN DEVICES ({len(unknown)}) — NOT in NetBox ---")
        print(f"{'IP':<18} {'MAC':<19} {'SSH Hostname':<40} {'SSH Serial'}")
        print("-" * 110)
        for r in sorted(unknown, key=lambda x: x["ip"]):
            hostname = r.get("ssh_hostname", "—")
            serial = r.get("ssh_serial", "—")
            print(f"  {r['ip']:<16} {r['mac']:<19} {hostname:<40} {serial}")

    # Summary
    missing_ip = [r for r in known if not r.get("nb_device_ip")]
    print(f"\n{'='*110}")
    print(f"Summary:")
    print(f"  Total discovered:          {len(all_results)}")
    print(f"  Matched in NetBox:         {len(known)}")
    print(f"  Unknown (not in NetBox):   {len(unknown)}")
    if missing_ip:
        print(f"  Known but MISSING mgmt IP: {len(missing_ip)} ← can be populated now")
    print(f"{'='*110}")


if __name__ == "__main__":
    main()

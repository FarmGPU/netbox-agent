"""ARP neighbor discovery and reporting to bmc-api.

Scans local network interfaces for MAC→IP neighbor pairs using arp-scan
(preferred) with fallback to kernel neighbor table (ip neigh show).
Posts discovered pairs to bmc-api's /arp-pairs endpoint for reconciliation
against NetBox.
"""

import json
import logging
import os
import re
import shutil
import socket
import subprocess

import requests


def _has_arp_scan() -> bool:
    """Check if arp-scan is installed."""
    return shutil.which("arp-scan") is not None


def _scan_arp_scan(interface: str, timeout: int) -> list[tuple[str, str]]:
    """Run arp-scan on an interface and return (MAC, IP) pairs.

    Args:
        interface: Network interface name (e.g., "ens4035f0np0").
        timeout: Timeout in seconds for the scan.

    Returns:
        List of (MAC, IP) tuples discovered on the interface.
    """
    pairs = []
    try:
        result = subprocess.run(
            ["arp-scan", "--localnet", f"--interface={interface}",
             f"--timeout={timeout * 1000}", "--plain"],
            capture_output=True, text=True, timeout=timeout + 10,
        )
        for line in result.stdout.strip().splitlines():
            # arp-scan --plain output: IP\tMAC\tVendor (or IP\tMAC)
            parts = line.split("\t")
            if len(parts) >= 2:
                ip, mac = parts[0].strip(), parts[1].strip()
                # Basic sanity: MAC should be 17 chars (XX:XX:XX:XX:XX:XX)
                if len(mac) == 17 and ":" in mac:
                    pairs.append((mac.upper(), ip))
    except subprocess.TimeoutExpired:
        logging.warning("arp-scan timed out on %s after %ds", interface, timeout)
    except Exception as exc:
        logging.warning("arp-scan failed on %s: %s", interface, exc)
    return pairs


def _scan_ip_neigh() -> list[tuple[str, str]]:
    """Parse kernel neighbor table as fallback.

    Uses 'ip -j neigh show' for JSON output. Only accepts REACHABLE
    entries — STALE entries are excluded because they may hold outdated
    IPs that could revert correct updates in NetBox.

    Returns:
        List of (MAC, IP) tuples from the neighbor table.
    """
    pairs = []
    try:
        result = subprocess.run(
            ["ip", "-j", "neigh", "show"],
            capture_output=True, text=True, timeout=10,
        )
        entries = json.loads(result.stdout) if result.stdout.strip() else []
        for entry in entries:
            state = entry.get("state", [])
            # state can be a list of strings like ["REACHABLE"] or ["STALE"]
            if isinstance(state, str):
                state = [state]
            # Only REACHABLE — stale entries risk reporting outdated IPs
            if "REACHABLE" not in state:
                continue
            mac = entry.get("lladdr", "")
            ip = entry.get("dst", "")
            if mac and ip and len(mac) == 17:
                pairs.append((mac.upper(), ip))
    except Exception as exc:
        logging.warning("ip neigh show failed: %s", exc)
    return pairs


def _get_scan_interfaces(config) -> list[str]:
    """Determine which interfaces to scan.

    If config.arp_report.interfaces is set, use those. Otherwise enumerate
    non-ignored interfaces from /sys/class/net/ that are UP and have an
    IPv4 address.

    Args:
        config: Parsed netbox-agent config namespace.

    Returns:
        List of interface names to scan.
    """
    # Explicit interface list from config
    configured = getattr(config.arp_report, "interfaces", "")
    if configured:
        return [i.strip() for i in configured.split(",") if i.strip()]

    # Auto-detect: enumerate interfaces matching scan() logic from network.py
    ignore_re = getattr(config.network, "ignore_interfaces", r"(dummy.*|docker.*)")
    interfaces = []

    for iface in os.listdir("/sys/class/net/"):
        if not os.path.islink(f"/sys/class/net/{iface}"):
            continue
        if ignore_re and re.match(ignore_re, iface):
            continue
        # Check interface is UP
        try:
            flags_path = f"/sys/class/net/{iface}/flags"
            with open(flags_path) as f:
                flags = int(f.read().strip(), 16)
            if not (flags & 0x1):  # IFF_UP
                continue
        except (OSError, ValueError):
            continue
        # Check for IPv4 address (skip interfaces with no IP)
        try:
            import netifaces
            addrs = netifaces.ifaddresses(iface).get(netifaces.AF_INET, [])
            if not addrs:
                continue
        except Exception:
            continue
        interfaces.append(iface)

    return interfaces


def scan_and_report(config) -> dict:
    """Scan ARP neighbors and report to bmc-api.

    Main entry point. Determines interfaces, scans each one (arp-scan
    preferred, ip neigh fallback), deduplicates, and POSTs to bmc-api.

    Args:
        config: Parsed netbox-agent config namespace.

    Returns:
        Summary dict with interfaces_scanned, pairs_found,
        pairs_submitted, and response.

    Raises:
        Nothing — logs errors but does not raise (non-fatal).
    """
    bmc_api_url = getattr(config.arp_report, "bmc_api_url", "http://localhost:8100")
    bmc_api_key = getattr(config.arp_report, "bmc_api_key", "")
    scan_timeout = getattr(config.arp_report, "scan_timeout", 30)

    interfaces = _get_scan_interfaces(config)
    use_arp_scan = _has_arp_scan()

    all_pairs: dict[str, str] = {}  # MAC → IP (dedup: last seen wins)

    if use_arp_scan:
        for iface in interfaces:
            pairs = _scan_arp_scan(iface, scan_timeout)
            for mac, ip in pairs:
                all_pairs[mac] = ip
            logging.debug("arp-scan on %s: %d pairs", iface, len(pairs))
    else:
        # Fallback: ip neigh covers all interfaces at once
        logging.info("arp-scan not found, falling back to ip neigh show")
        pairs = _scan_ip_neigh()
        for mac, ip in pairs:
            all_pairs[mac] = ip

    pairs_list = [{"mac": mac, "ip": ip} for mac, ip in all_pairs.items()]
    hostname = socket.gethostname()

    result = {
        "interfaces_scanned": len(interfaces),
        "pairs_found": len(all_pairs),
        "pairs_submitted": 0,
        "response": None,
        "method": "arp-scan" if use_arp_scan else "ip-neigh",
    }

    if not pairs_list:
        logging.info("ARP report: no pairs found, skipping POST")
        return result

    # POST to bmc-api
    url = f"{bmc_api_url.rstrip('/')}/arp-pairs"
    headers = {"Content-Type": "application/json"}
    if bmc_api_key:
        headers["Authorization"] = f"Bearer {bmc_api_key}"

    try:
        resp = requests.post(
            url,
            json={"pairs": pairs_list, "hostname": hostname},
            headers=headers,
            timeout=30,
        )
        result["pairs_submitted"] = len(pairs_list)
        result["response"] = resp.json() if resp.ok else {"status_code": resp.status_code, "text": resp.text}
        if not resp.ok:
            logging.warning("ARP report POST failed: HTTP %d — %s", resp.status_code, resp.text)
        else:
            logging.info("ARP report: submitted %d pairs to %s", len(pairs_list), url)
    except requests.RequestException as exc:
        logging.warning("ARP report POST failed: %s", exc)
        result["response"] = {"error": str(exc)}

    return result

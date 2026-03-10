"""ARP neighbor discovery and reporting to bmc-api.

Scans local network interfaces for MAC→IP neighbor pairs using a 3-tier
fallback chain:

  1. arp-scan  — active ARP, best coverage (~3s /24)
  2. nmap -sn  — active ARP via ping scan, good coverage (~10-15s /24)
  3. ip neigh  — passive kernel cache, REACHABLE only (instant but poor)

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
import xml.etree.ElementTree as ET

import requests


def _has_arp_scan() -> bool:
    """Check if arp-scan is installed."""
    return shutil.which("arp-scan") is not None


def _scan_arp_scan(interface: str, timeout: int) -> list[tuple[str, str]]:
    """Run arp-scan on an interface and return (MAC, IP) pairs.

    Args:
        interface: Network interface name (e.g., "ens4035f0np0").
        timeout: Total timeout in seconds for the scan subprocess.
            arp-scan's per-host probe timeout is fixed at 500ms (default).
            This timeout controls how long we wait for the entire scan to
            complete before killing the subprocess.

    Returns:
        List of (MAC, IP) tuples discovered on the interface.
    """
    pairs = []
    try:
        result = subprocess.run(
            ["arp-scan", "--localnet", f"--interface={interface}", "--plain"],
            capture_output=True, text=True, timeout=timeout,
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


def _has_nmap() -> bool:
    """Check if nmap is installed."""
    return shutil.which("nmap") is not None


def _get_interface_cidr(interface: str) -> str | None:
    """Derive the CIDR subnet for an interface (e.g. '10.100.192.0/24').

    Uses netifaces for the IPv4 address and netaddr for CIDR calculation.
    Both are existing project dependencies.

    Returns:
        CIDR string, or None if the interface has no IPv4 address.
    """
    try:
        import netifaces
        from netaddr import IPNetwork

        addrs = netifaces.ifaddresses(interface).get(netifaces.AF_INET, [])
        if not addrs:
            return None
        addr = addrs[0]["addr"]
        # netifaces uses "netmask", netifaces2 uses "mask"
        netmask = addrs[0].get("netmask") or addrs[0].get("mask")
        network = IPNetwork(f"{addr}/{netmask}")
        return str(network.cidr)
    except Exception as exc:
        logging.warning("Could not determine CIDR for %s: %s", interface, exc)
        return None


def _scan_nmap(interface: str, timeout: int) -> list[tuple[str, str]]:
    """Run nmap -sn on an interface and return (MAC, IP) pairs.

    nmap's -sn (ping scan) performs ARP discovery when run as root on a
    local subnet.  XML output (-oX -) is parsed for host entries that
    contain both an ipv4 and a mac address element.

    Args:
        interface: Network interface name (e.g., "ens4035f0np0").
        timeout: Total timeout in seconds for the scan subprocess.

    Returns:
        List of (MAC, IP) tuples discovered on the interface.
    """
    pairs: list[tuple[str, str]] = []
    cidr = _get_interface_cidr(interface)
    if cidr is None:
        logging.warning("nmap: skipping %s — no CIDR available", interface)
        return pairs
    try:
        result = subprocess.run(
            ["nmap", "-sn", "-oX", "-", "-e", interface, cidr],
            capture_output=True, text=True, timeout=timeout,
        )
        root = ET.fromstring(result.stdout)
        for host in root.findall("host"):
            ipv4 = None
            mac = None
            for addr in host.findall("address"):
                if addr.get("addrtype") == "ipv4":
                    ipv4 = addr.get("addr")
                elif addr.get("addrtype") == "mac":
                    mac = addr.get("addr")
            if mac and ipv4 and len(mac) == 17 and ":" in mac:
                pairs.append((mac.upper(), ipv4))
    except subprocess.TimeoutExpired:
        logging.warning("nmap timed out on %s after %ds", interface, timeout)
    except Exception as exc:
        logging.warning("nmap scan failed on %s: %s", interface, exc)
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

    for iface in sorted(os.listdir("/sys/class/net/")):
        if not os.path.islink(f"/sys/class/net/{iface}"):
            continue
        # Always skip loopback — scanning 127.0.0.0/8 is useless and slow
        if iface == "lo":
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
    use_nmap = not use_arp_scan and _has_nmap()

    all_pairs: dict[str, str] = {}  # MAC → IP (dedup: last seen wins)

    if use_arp_scan:
        for iface in interfaces:
            pairs = _scan_arp_scan(iface, scan_timeout)
            for mac, ip in pairs:
                all_pairs[mac] = ip
            logging.debug("arp-scan on %s: %d pairs", iface, len(pairs))
        method = "arp-scan"
    elif use_nmap:
        logging.info("arp-scan not found, using nmap -sn for ARP discovery")
        for iface in interfaces:
            pairs = _scan_nmap(iface, scan_timeout)
            for mac, ip in pairs:
                all_pairs[mac] = ip
            logging.debug("nmap on %s: %d pairs", iface, len(pairs))
        method = "nmap"
    else:
        # Last resort: ip neigh covers all interfaces at once
        logging.info("arp-scan and nmap not found, falling back to ip neigh show")
        pairs = _scan_ip_neigh()
        for mac, ip in pairs:
            all_pairs[mac] = ip
        method = "ip-neigh"

    pairs_list = [{"mac": mac, "ip": ip} for mac, ip in all_pairs.items()]
    hostname = socket.gethostname()

    result = {
        "interfaces_scanned": len(interfaces),
        "pairs_found": len(all_pairs),
        "pairs_submitted": 0,
        "response": None,
        "method": method,
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
        post_timeout = getattr(config.arp_report, "post_timeout", 120)
        resp = requests.post(
            url,
            json={"pairs": pairs_list, "hostname": hostname},
            headers=headers,
            timeout=post_timeout,
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

#!/usr/bin/env python3
"""
Validate BMC credentials from machines.csv against live BMC endpoints.

For each entry with a BMC IP and password, tries:
  1. Redfish GET /redfish/v1/ with username ADMIN
  2. Redfish GET /redfish/v1/ with username admin
  3. IPMI chassis status with username ADMIN
  4. IPMI chassis status with username admin

Reports which username/protocol works for each device.
Outputs a validated CSV suitable for merging into the credential sync pipeline.

Usage:
  python3 validate_machines_csv.py                         # validate all
  python3 validate_machines_csv.py --workers 5             # limit concurrency
  python3 validate_machines_csv.py --output results.csv    # custom output path
"""

import argparse
import csv
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

MACHINES_CSV = os.path.expanduser("~/machines.csv")

# Cross-reference for asset tags
HOSTNAMES_CSV = os.path.join(
    os.path.expanduser("~"), "asset-tag-testing", "csvs", "compute", "hostnames.csv"
)

USERNAMES_TO_TRY = ["ADMIN", "admin"]
TIMEOUT_SECS = 10


def load_mac_to_asset_tag(path):
    """Build MAC -> asset_tag index from hostnames.csv."""
    index = {}
    if not os.path.exists(path):
        return index
    with open(path, newline="") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            if len(row) < 19:
                continue
            tag = row[5].strip()
            if not tag:
                continue
            for col in [17, 18]:
                mac = row[col].strip().lower().replace(":", "").replace("-", "")
                if mac:
                    index[mac] = tag
    return index


def load_machines_csv(path, mac_index):
    """Parse machines.csv, return list of entry dicts."""
    entries = []
    with open(path, newline="") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            if not any(c.strip() for c in row[:7]):
                continue
            pw = row[4].strip()
            if not pw:
                continue
            hostname = row[0].strip()
            serial = row[2].strip()
            mac_raw = row[3].strip()
            mac_norm = mac_raw.lower().replace(":", "").replace("-", "")
            bmc_ip = row[6].strip()
            model = row[1].strip()

            # Derive friendly name
            if hostname:
                friendly = hostname.split("-")[-1]
            elif serial:
                friendly = f"SN:{serial[:12]}"
            else:
                friendly = f"MAC:{mac_raw[:12]}"

            asset_tag = mac_index.get(mac_norm, "")

            entries.append({
                "friendly": friendly,
                "hostname": hostname,
                "serial": serial,
                "mac": mac_raw,
                "mac_norm": mac_norm,
                "bmc_ip": bmc_ip,
                "password": pw,
                "model": model,
                "asset_tag": asset_tag,
            })
    return entries


def try_redfish(ip, username, password):
    """Try Redfish GET /redfish/v1/Systems/ — returns True if auth succeeds.

    Note: /redfish/v1/ (root) is unauthenticated on SuperMicro BMCs and
    returns 200 regardless of credentials.  /redfish/v1/Systems/ requires
    valid auth and returns 401 on failure.
    """
    try:
        result = subprocess.run(
            [
                "curl", "-sk", "--connect-timeout", str(TIMEOUT_SECS),
                "--max-time", str(TIMEOUT_SECS),
                "-o", "/dev/null", "-w", "%{http_code}",
                "-u", f"{username}:{password}",
                f"https://{ip}/redfish/v1/Systems/",
            ],
            capture_output=True, text=True, timeout=TIMEOUT_SECS + 5,
        )
        code = result.stdout.strip()
        return code == "200"
    except Exception:
        return False


def try_ipmi(ip, username, password):
    """Try IPMI chassis status — returns True if auth succeeds."""
    try:
        result = subprocess.run(
            [
                "ipmitool", "-I", "lanplus",
                "-H", ip, "-U", username, "-P", password,
                "chassis", "status",
            ],
            capture_output=True, text=True, timeout=TIMEOUT_SECS + 5,
        )
        return result.returncode == 0 and "Power" in result.stdout
    except Exception:
        return False


def validate_entry(entry):
    """
    Validate a single entry. Try Redfish then IPMI with each username.
    Returns the entry dict augmented with validation results.
    """
    ip = entry["bmc_ip"]
    pw = entry["password"]
    name = entry["friendly"]

    if not ip or ip.lower() in ("check", ""):
        entry["status"] = "NO_IP"
        entry["validated_user"] = ""
        entry["validated_protocol"] = ""
        return entry

    # Try each username with Redfish first, then IPMI
    for username in USERNAMES_TO_TRY:
        if try_redfish(ip, username, pw):
            entry["status"] = "OK"
            entry["validated_user"] = username
            entry["validated_protocol"] = "redfish"
            return entry

    for username in USERNAMES_TO_TRY:
        if try_ipmi(ip, username, pw):
            entry["status"] = "OK"
            entry["validated_user"] = username
            entry["validated_protocol"] = "ipmi"
            return entry

    entry["status"] = "FAILED"
    entry["validated_user"] = ""
    entry["validated_protocol"] = ""
    return entry


def main():
    parser = argparse.ArgumentParser(description="Validate machines.csv BMC credentials")
    parser.add_argument("--machines-csv", default=MACHINES_CSV)
    parser.add_argument("--hostnames-csv", default=HOSTNAMES_CSV)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--output",
        default=os.path.join(
            os.path.expanduser("~"),
            "asset-tag-testing", "csvs", "compute", "machines_validated.csv",
        ),
    )
    args = parser.parse_args()

    mac_index = load_mac_to_asset_tag(args.hostnames_csv)
    entries = load_machines_csv(args.machines_csv, mac_index)

    testable = [e for e in entries if e["bmc_ip"] and e["bmc_ip"].lower() not in ("check", "")]
    untestable = [e for e in entries if e not in testable]

    print(f"Loaded {len(entries)} entries from machines.csv")
    print(f"  {len(testable)} with BMC IPs (will test)")
    print(f"  {len(untestable)} without BMC IPs (skipped)")
    print(f"  {sum(1 for e in entries if e['asset_tag'])} matched to asset tags")
    print(f"\nValidating with {args.workers} workers...\n")

    results = []
    ok = 0
    failed = 0
    no_ip = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(validate_entry, e): e for e in entries}
        for future in as_completed(futures):
            entry = future.result()
            results.append(entry)

            tag_str = f"[{entry['asset_tag']}]" if entry["asset_tag"] else "[no-tag]"
            if entry["status"] == "OK":
                ok += 1
                print(
                    f"  ✓ {entry['friendly']:<14} {tag_str:<10} "
                    f"{entry['validated_user']}@{entry['bmc_ip']} "
                    f"via {entry['validated_protocol']}"
                )
            elif entry["status"] == "NO_IP":
                no_ip += 1
                print(f"  - {entry['friendly']:<14} {tag_str:<10} (no BMC IP)")
            else:
                failed += 1
                print(
                    f"  ✗ {entry['friendly']:<14} {tag_str:<10} "
                    f"FAILED @ {entry['bmc_ip']}"
                )

    # Sort results by friendly name for consistent output
    results.sort(key=lambda e: e["friendly"])

    print(f"\n{'='*60}")
    print(f"Results: {ok} OK, {failed} FAILED, {no_ip} no IP")
    print(f"{'='*60}")

    # Write validated CSV
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "hostname", "friendly", "model", "serial", "bmc_mac", "bmc_ip",
            "asset_tag", "validated_user", "validated_password",
            "validated_protocol", "status",
        ])
        for e in results:
            # Always write the password from machines.csv — even for
            # FAILED/NO_IP entries.  The password came from the CSV and
            # is likely correct; failure just means the BMC was unreachable.
            writer.writerow([
                e["hostname"], e["friendly"], e["model"], e["serial"],
                e["mac"], e["bmc_ip"], e["asset_tag"],
                e["validated_user"] or "ADMIN",
                e["password"],
                e.get("validated_protocol", ""), e["status"],
            ])

    print(f"\nWrote: {args.output}")
    print(f"  {ok} entries with validated credentials")
    if failed:
        print(f"  {failed} entries FAILED — check BMC IP / password / reachability")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

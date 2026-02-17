#!/usr/bin/env python3
"""
Sync PDU asset tags from CSV into NetBox with live serial verification.

General-purpose: works with any set of ServerTech PDUs. Driven entirely by
two input CSVs — no device-specific logic is hard-coded.

Four phases:
  1. Load CSV data (hostnames.csv + validated_hosts.csv), merge by serial
  2. Pre-flight: read serial from each live PDU via HTTP, compare to CSV
  3. Update asset_tag in NetBox (with belt-and-suspenders serial cross-check)
  4. Post-update verification — re-read from NetBox and confirm

CSV formats:
  hostnames.csv (authoritative asset tag source):
    Columns by index:  1=hostname, 5=asset_tag, 15=chassis_sn, 20=mgmt_ip
    Rows WITH a serial   → eligible for live verification
    Rows WITHOUT a serial → "unverifiable" (tagged by hostname only)

  validated_hosts.csv (live-discovered PDUs):
    DictReader columns: hostname, mgmt_ip, chassis_sn, validated
    Provides the live IP for reaching each PDU by serial match.

Modes:
  --verified-only        Only program PDUs whose serials were live-verified.
                         This is the safest mode for production use.
  --dry-run              Preview all changes without writing to NetBox.
  --skip-live-verify     Trust CSV serials without HTTP verification.

Examples:
  # Preview what would happen for verified PDUs only
  python3 sync_asset_tags.py --verified-only --dry-run

  # Program only the serial-verified PDUs (production safe)
  python3 sync_asset_tags.py --verified-only

  # Program everything including unverifiable PDUs
  python3 sync_asset_tags.py

  # Use custom CSV paths
  python3 sync_asset_tags.py --verified-only \\
      --hostnames-csv /path/to/hostnames.csv \\
      --validated-csv /path/to/validated_hosts.csv
"""

import argparse
import csv
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import urllib3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "schema"))
from nb_connection import get_api, logger

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from servertech import ServerTechClient

# ── Default CSV paths ────────────────────────────────────────────────────────
CSV_DIR = os.path.join(os.path.expanduser("~"), "asset-tag-testing", "csvs", "pdus")
HOSTNAMES_CSV = os.path.join(CSV_DIR, "hostnames.csv")
VALIDATED_CSV = os.path.join(CSV_DIR, "validated_hosts.csv")

LIVE_VERIFY_WORKERS = 10


# ── Phase 1: Load and merge CSV data ─────────────────────────────────────────

def load_hostnames_csv(path):
    """
    Read hostnames.csv and return {serial: record} plus a list of
    unverifiable entries (rows with no serial number).

    Column layout (by index):
      1  = Legacy Hostname (device name in NetBox)
      5  = Asset Tag (base-36)
      15 = Chassis SN (serial number)
      20 = BMC IP (NetBox) — may have /24 suffix
    """
    by_serial = {}
    unverifiable = []

    with open(path, newline="") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            if len(row) < 21:
                continue
            hostname = row[1].strip()
            asset_tag = row[5].strip()
            serial = row[15].strip()
            csv_ip_raw = row[20].strip()
            csv_ip = csv_ip_raw.split("/")[0] if csv_ip_raw else ""

            if not hostname or not asset_tag:
                continue

            if serial:
                by_serial[serial] = {
                    "hostname": hostname,
                    "asset_tag": asset_tag,
                    "serial": serial,
                    "csv_ip": csv_ip,
                }
            else:
                unverifiable.append({
                    "hostname": hostname,
                    "asset_tag": asset_tag,
                    "serial": "",
                    "live_ip": "",
                    "reason": "No serial in CSV",
                })

    return by_serial, unverifiable


def load_validated_csv(path):
    """
    Read validated_hosts.csv and return {serial: {live_ip, mac, hostname}}
    for rows that have a chassis_sn and a reachable IP.
    """
    by_serial = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            serial = row.get("chassis_sn", "").strip()
            if not serial:
                continue
            ip = row.get("mgmt_ip", "").strip()
            validated = row.get("validated", "").strip().lower()
            if validated == "yes" or ip:
                by_serial[serial] = {
                    "live_ip": ip,
                    "mac": row.get("mgmt_mac", "").strip(),
                    "hostname": row.get("hostname", "").strip(),
                }
    return by_serial


def merge_csv_data(hostnames_path, validated_path):
    """
    Merge the two CSVs by serial number.

    Returns:
      verifiable   — list of dicts with live_ip for HTTP serial verification
      unverifiable — list of dicts without live_ip (no live verification possible)
    """
    hn_by_serial, unverifiable = load_hostnames_csv(hostnames_path)
    val_by_serial = load_validated_csv(validated_path)

    verifiable = []
    for serial, hn_rec in hn_by_serial.items():
        val_rec = val_by_serial.get(serial)
        live_ip = val_rec["live_ip"] if val_rec else ""

        # Fallback: use CSV IP from hostnames.csv if validated didn't supply one
        if not live_ip:
            live_ip = hn_rec.get("csv_ip", "")

        if live_ip:
            verifiable.append({
                "hostname": hn_rec["hostname"],
                "asset_tag": hn_rec["asset_tag"],
                "serial": serial,
                "live_ip": live_ip,
            })
        else:
            unverifiable.append({
                "hostname": hn_rec["hostname"],
                "asset_tag": hn_rec["asset_tag"],
                "serial": serial,
                "live_ip": "",
                "reason": "No reachable IP",
            })

    return verifiable, unverifiable


# ── Phase 2: Pre-flight serial verification ──────────────────────────────────

def verify_one_serial(entry):
    """Fetch serial from live PDU via HTTP and compare to CSV serial."""
    ip = entry["live_ip"]
    expected = entry["serial"]
    client = ServerTechClient(ip)
    live_serial, error = client.get_serial()

    entry["live_serial"] = live_serial
    entry["verify_error"] = error
    if error:
        entry["status"] = f"ERROR: {error}"
    elif live_serial == expected:
        entry["status"] = "PASS"
    else:
        entry["status"] = "MISMATCH"
    return entry


def run_live_verification(verifiable):
    """
    Verify serials in parallel via HTTP.
    Returns (results, had_failures).
    """
    print(f"\n{'='*80}")
    print("Phase 2: Live serial verification")
    print(f"{'='*80}")
    print(f"Verifying {len(verifiable)} PDUs (workers={LIVE_VERIFY_WORKERS})...\n")

    with ThreadPoolExecutor(max_workers=LIVE_VERIFY_WORKERS) as pool:
        futures = {pool.submit(verify_one_serial, e): e for e in verifiable}
        results = []
        for fut in as_completed(futures):
            results.append(fut.result())

    results.sort(key=lambda e: e["hostname"])

    hdr = f"{'Hostname':<30} {'CSV Serial':<12} {'Live Serial':<12} {'IP':<18} {'Status'}"
    print(hdr)
    print("-" * len(hdr))

    had_failures = False
    for e in results:
        if e["status"] == "PASS":
            icon = "\u2705 PASS"
        else:
            icon = "\u274c " + e["status"]
            had_failures = True
        print(f"{e['hostname']:<30} {e['serial']:<12} "
              f"{e.get('live_serial', ''):<12} {e['live_ip']:<18} {icon}")

    return results, had_failures


# ── Phase 3: NetBox update ───────────────────────────────────────────────────

def _get_api_session():
    """
    Build a requests.Session for direct NetBox API calls.

    pynetbox's device.save() triggers full-object validation, which fails
    when required custom fields (e.g. 'owner') are empty on legacy records.
    Direct PATCH with only the fields we're changing sidesteps this — but
    NetBox still requires that any required custom field be non-empty, so we
    include a default 'owner' value when one is missing.
    """
    url = os.environ.get("NETBOX_URL", "")
    token = os.environ.get("NETBOX_TOKEN", "")
    ssl_verify = os.environ.get("NETBOX_SSL_VERIFY", "true").lower() != "false"

    if not ssl_verify:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    session = requests.Session()
    session.verify = ssl_verify
    session.headers.update({
        "Authorization": f"Token {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    return session, url


# Default owner for PDUs that have no owner set (required custom field)
DEFAULT_OWNER = "FarmGPU"


def update_device_asset_tag(nb, hostname, expected_serial, asset_tag, dry_run):
    """
    Find device by name in NetBox, cross-check serial, set asset_tag.

    Uses direct PATCH to avoid full-object validation failures when required
    custom fields are empty on legacy records.  If 'owner' is unset, it is
    populated with DEFAULT_OWNER alongside the asset_tag update.

    Returns (success, message).
    """
    devices = list(nb.dcim.devices.filter(name=hostname))
    if len(devices) != 1:
        return False, f"Expected 1 device named '{hostname}', found {len(devices)}"

    device = devices[0]

    # Belt-and-suspenders: if we have an expected serial, confirm NetBox agrees
    if expected_serial and device.serial and device.serial != expected_serial:
        return False, (f"Serial mismatch in NetBox: expected '{expected_serial}', "
                       f"got '{device.serial}'")

    if device.asset_tag == asset_tag:
        return True, f"Already set to '{asset_tag}'"

    if dry_run:
        return True, f"Would set asset_tag='{asset_tag}' (current: '{device.asset_tag}')"

    # Build PATCH payload
    payload = {"asset_tag": asset_tag}

    # If required 'owner' custom field is empty, set it to avoid validation error
    owner = (device.custom_fields or {}).get("owner")
    if not owner:
        payload["custom_fields"] = {"owner": DEFAULT_OWNER}

    try:
        session, base_url = _get_api_session()
        resp = session.patch(
            f"{base_url}/api/dcim/devices/{device.id}/",
            json=payload,
        )
        if resp.status_code == 200:
            extra = f" (also set owner={DEFAULT_OWNER})" if "custom_fields" in payload else ""
            return True, f"Set asset_tag='{asset_tag}'{extra}"
        else:
            return False, f"HTTP {resp.status_code}: {resp.text[:200]}"
    except Exception as e:
        return False, f"Request failed: {e}"


def run_netbox_updates(nb, entries, dry_run):
    """
    Phase 3: Update asset_tags in NetBox for the given entries.
    """
    print(f"\n{'='*80}")
    print("Phase 3: NetBox asset tag updates")
    print(f"{'='*80}\n")

    successes = 0
    failures = 0

    for entry in sorted(entries, key=lambda e: e["hostname"]):
        hostname = entry["hostname"]
        asset_tag = entry["asset_tag"]
        serial = entry.get("serial", "")

        ok, msg = update_device_asset_tag(nb, hostname, serial, asset_tag, dry_run)
        status = "OK" if ok else "FAIL"
        if ok:
            successes += 1
        else:
            failures += 1
        print(f"  [{status}] {hostname:<30} tag={asset_tag:<6} serial={serial or '(none)':<10} {msg}")

    print(f"\nResults: {successes} succeeded, {failures} failed (of {len(entries)} total)")
    return failures == 0


# ── Phase 4: Post-update verification ────────────────────────────────────────

def run_post_verification(nb, entries):
    """
    Re-read all updated devices from NetBox and confirm asset_tags match.
    """
    print(f"\n{'='*80}")
    print("Phase 4: Post-update verification")
    print(f"{'='*80}\n")

    ok_count = 0
    fail_count = 0

    for entry in sorted(entries, key=lambda e: e["hostname"]):
        hostname = entry["hostname"]
        expected_tag = entry["asset_tag"]

        devices = list(nb.dcim.devices.filter(name=hostname))
        if len(devices) != 1:
            print(f"  FAIL  {hostname:<30} device not found or ambiguous")
            fail_count += 1
            continue

        actual_tag = devices[0].asset_tag or ""
        if actual_tag == expected_tag:
            print(f"  OK    {hostname:<30} asset_tag={actual_tag}")
            ok_count += 1
        else:
            print(f"  FAIL  {hostname:<30} expected='{expected_tag}' got='{actual_tag}'")
            fail_count += 1

    print(f"\nVerification: {ok_count} OK, {fail_count} failed (of {len(entries)} total)")
    return fail_count == 0


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Sync PDU asset tags from CSV into NetBox with live serial verification.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  %(prog)s --verified-only --dry-run      # preview verified PDUs only
  %(prog)s --verified-only                # program verified PDUs (safest)
  %(prog)s --dry-run                      # preview all PDUs
  %(prog)s                                # program all PDUs
""")
    parser.add_argument("--dry-run", action="store_true",
                        help="show what would change, don't write to NetBox")
    parser.add_argument("--verified-only", action="store_true",
                        help="only program PDUs whose serials were live-verified "
                             "(skips unverifiable entries)")
    parser.add_argument("--skip-live-verify", action="store_true",
                        help="skip HTTP serial verification (trust CSV data)")
    parser.add_argument("--hostnames-csv", default=HOSTNAMES_CSV,
                        help=f"path to hostnames.csv (default: {HOSTNAMES_CSV})")
    parser.add_argument("--validated-csv", default=VALIDATED_CSV,
                        help=f"path to validated_hosts.csv (default: {VALIDATED_CSV})")
    args = parser.parse_args()

    if args.dry_run:
        print("=== DRY RUN — no changes will be made ===\n")

    # ── Phase 1 ──────────────────────────────────────────────────────────
    print(f"{'='*80}")
    print("Phase 1: Loading CSV data")
    print(f"{'='*80}")
    print(f"  hostnames.csv:  {args.hostnames_csv}")
    print(f"  validated.csv:  {args.validated_csv}")

    verifiable, unverifiable = merge_csv_data(args.hostnames_csv, args.validated_csv)

    print(f"\n  Verifiable (serial + live IP):  {len(verifiable)}")
    print(f"  Unverifiable (no IP/serial):   {len(unverifiable)}")
    for e in unverifiable:
        print(f"    {e['hostname']:<30} tag={e['asset_tag']:<6} — {e.get('reason', '')}")
    print(f"  Total in CSVs:                 {len(verifiable) + len(unverifiable)}")

    # ── Phase 2 ──────────────────────────────────────────────────────────
    if not args.skip_live_verify and verifiable:
        verifiable, had_failures = run_live_verification(verifiable)
        if had_failures:
            print("\nABORTING: Serial verification failures detected.")
            print("Fix CSV data or investigate mismatched PDUs before proceeding.")
            sys.exit(1)
        print(f"\nAll {len(verifiable)} serial verifications passed.")
    elif args.skip_live_verify:
        print("\n[--skip-live-verify] Skipping HTTP serial verification.")

    # ── Determine target set ─────────────────────────────────────────────
    if args.verified_only:
        targets = verifiable
        print(f"\n[--verified-only] Targeting {len(targets)} serial-verified PDUs only.")
        if unverifiable:
            print(f"  Skipping {len(unverifiable)} unverifiable entries.")
    else:
        targets = verifiable + unverifiable
        print(f"\nTargeting all {len(targets)} PDUs (verified + unverifiable).")

    if not targets:
        print("Nothing to do.")
        return

    # ── Phase 3 ──────────────────────────────────────────────────────────
    nb = get_api()
    all_ok = run_netbox_updates(nb, targets, args.dry_run)

    if args.dry_run:
        print("\n=== DRY RUN complete — re-run without --dry-run to execute ===")
        return

    if not all_ok:
        print("\nSome updates failed. Review output above.")
        sys.exit(1)

    # ── Phase 4 ──────────────────────────────────────────────────────────
    run_post_verification(nb, targets)


if __name__ == "__main__":
    main()

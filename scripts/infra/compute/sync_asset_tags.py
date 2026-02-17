#!/usr/bin/env python3
"""
Sync compute asset tags from CSV into NetBox via multi-strategy device matching.

Matching priority:
  1. BMC MAC address (CSV cols 17/18 vs NetBox bmc_mac_address CF + interface MACs)
  2. Chassis serial (CSV col 15 vs NetBox device.serial)
  3. Hostname variants (CSV col 1 + col 16 semicolon-separated vs NetBox device.name)

Four phases:
  1. Load CSV data (hostnames.csv), parse positional columns
  2. Match CSV entries to NetBox devices via MAC → Serial → Hostname
  3. Update asset_tag in NetBox (direct PATCH to avoid pynetbox validation)
  4. Post-update verification — re-read from NetBox and confirm

Examples:
  python3 sync_asset_tags.py --dry-run                    # preview
  python3 sync_asset_tags.py                              # execute
  python3 sync_asset_tags.py --hostnames-csv /custom.csv  # custom path
"""

import argparse
import csv
import os
import re
import sys

import requests
import urllib3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "schema"))
from nb_connection import get_api, logger

# ── Default CSV path ─────────────────────────────────────────────────────────
CSV_DIR = os.path.join(os.path.expanduser("~"), "asset-tag-testing", "csvs", "compute")
HOSTNAMES_CSV = os.path.join(CSV_DIR, "hostnames.csv")

# Device roles to load from NetBox
DEVICE_ROLES = ["server", "gpu-server", "storage-server", "farmgpu-infrastructure"]

# Default owner for devices that have no owner set (required custom field)
DEFAULT_OWNER = "FarmGPU"


# ── Helpers ──────────────────────────────────────────────────────────────────

def normalize_mac(mac: str) -> str:
    """Strip separators and lowercase: '3C:EC:EF:33:8C:EC' → '3cecef338cec'."""
    return re.sub(r"[:.\-]", "", mac).lower().strip()


# ── Phase 1: Load CSV ────────────────────────────────────────────────────────

def load_hostnames_csv(path):
    """
    Read hostnames.csv and return (entries, skip_count).

    Column layout (by index):
      0  = Asset Tag Programmed? (Y/N)
      1  = Legacy Hostname
      2  = Full Hostname (new standardized name)
      5  = Asset Tag (base-36)
      15 = Chassis SN
      16 = Legacy/Known Hostnames (semicolon-separated)
      17 = BMC MAC (FarmGPU)
      18 = BMC MAC (NetBox)
    """
    entries = []
    skipped = 0

    with open(path, newline="") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row_num, row in enumerate(reader, start=2):
            if len(row) < 19:
                skipped += 1
                continue

            asset_tag = row[5].strip()
            if not asset_tag:
                skipped += 1
                continue

            hostname = row[1].strip()
            serial = row[15].strip()

            # Collect all MACs (both FarmGPU and NetBox columns)
            macs_raw = []
            if row[17].strip():
                macs_raw.append(row[17].strip())
            if row[18].strip():
                macs_raw.append(row[18].strip())
            macs = list({normalize_mac(m) for m in macs_raw if normalize_mac(m)})

            # Collect hostname variants from col 16 (semicolon-separated)
            hostname_variants = set()
            if hostname:
                hostname_variants.add(hostname.lower())
            for variant in row[16].strip().split(";"):
                v = variant.strip()
                if v:
                    hostname_variants.add(v.lower())

            # Skip rows with NO identifiers at all (pecan06-10 placeholder rows)
            if not hostname and not macs and not serial:
                skipped += 1
                continue

            entries.append({
                "row": row_num,
                "hostname": hostname,
                "full_hostname": row[2].strip(),
                "asset_tag": asset_tag,
                "serial": serial,
                "macs": macs,
                "hostname_variants": hostname_variants,
            })

    return entries, skipped


# ── Phase 2: Match to NetBox ─────────────────────────────────────────────────

def load_netbox_devices(nb):
    """Load all devices from target roles with their MACs, serial, and name."""
    devices = []
    for role in DEVICE_ROLES:
        for dev in nb.dcim.devices.filter(role=role):
            # Collect MACs: bmc_mac_address CF + all interface MACs
            dev_macs = set()
            bmc_mac = (dev.custom_fields or {}).get("bmc_mac_address", "")
            if bmc_mac:
                dev_macs.add(normalize_mac(bmc_mac))

            for iface in nb.dcim.interfaces.filter(device_id=dev.id):
                if iface.mac_address:
                    dev_macs.add(normalize_mac(str(iface.mac_address)))

            devices.append({
                "id": dev.id,
                "name": dev.name or "",
                "serial": dev.serial or "",
                "asset_tag": dev.asset_tag or "",
                "owner": (dev.custom_fields or {}).get("owner", ""),
                "macs": dev_macs,
            })

    return devices


def build_match_indexes(nb_devices):
    """
    Build 3 lookup indexes from NetBox devices:
      mac_index:    normalized_mac → device dict
      serial_index: serial → device dict
      name_index:   lowercased_name → device dict
    """
    mac_index = {}
    serial_index = {}
    name_index = {}

    for dev in nb_devices:
        for mac in dev["macs"]:
            if mac and mac not in mac_index:
                mac_index[mac] = dev
        if dev["serial"]:
            serial_index[dev["serial"]] = dev
        if dev["name"]:
            name_index[dev["name"].lower()] = dev

    return mac_index, serial_index, name_index


def match_csv_to_netbox(csv_entries, mac_index, serial_index, name_index):
    """
    Match each CSV entry to a NetBox device using priority: MAC → Serial → Hostname.

    Returns (matches, unmatched).
    Each match is a dict with csv entry fields plus nb_device, match_method.
    Conflict handling: if two CSV entries match the same NB device, first wins.
    """
    matches = []
    unmatched = []
    claimed_device_ids = {}  # device_id → csv entry that claimed it

    for entry in csv_entries:
        matched_dev = None
        method = None

        # 1. MAC match
        for mac in entry["macs"]:
            if mac in mac_index:
                matched_dev = mac_index[mac]
                method = "MAC"
                break

        # 2. Serial match
        if not matched_dev and entry["serial"]:
            if entry["serial"] in serial_index:
                matched_dev = serial_index[entry["serial"]]
                method = "Serial"

        # 3. Hostname match (try all variants)
        if not matched_dev:
            for variant in entry["hostname_variants"]:
                if variant in name_index:
                    matched_dev = name_index[variant]
                    method = "Hostname"
                    break

        if not matched_dev:
            unmatched.append(entry)
            continue

        # Conflict check: has another CSV entry already claimed this device?
        dev_id = matched_dev["id"]
        if dev_id in claimed_device_ids:
            prior = claimed_device_ids[dev_id]
            label = entry["hostname"] or entry["full_hostname"]
            prior_label = prior["hostname"] or prior["full_hostname"]
            logger.warning(
                "CONFLICT: CSV '%s' (tag=%s) matches NB device '%s' (id=%d) "
                "already claimed by CSV '%s' (tag=%s) — skipping",
                label, entry["asset_tag"],
                matched_dev["name"], dev_id,
                prior_label, prior["asset_tag"],
            )
            unmatched.append(entry)
            continue

        match_record = {
            **entry,
            "nb_device": matched_dev,
            "match_method": method,
        }
        matches.append(match_record)
        claimed_device_ids[dev_id] = entry

    return matches, unmatched


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


def update_device_asset_tag(device_id, device_name, asset_tag, owner, dry_run):
    """
    Set asset_tag on a NetBox device via direct PATCH.

    If 'owner' is empty, also sets it to DEFAULT_OWNER.
    Returns (success, message).
    """
    if dry_run:
        return True, f"Would set asset_tag='{asset_tag}'"

    payload = {"asset_tag": asset_tag}
    if not owner:
        payload["custom_fields"] = {"owner": DEFAULT_OWNER}

    try:
        session, base_url = _get_api_session()
        resp = session.patch(
            f"{base_url}/api/dcim/devices/{device_id}/",
            json=payload,
        )
        if resp.status_code == 200:
            extra = f" (also set owner={DEFAULT_OWNER})" if "custom_fields" in payload else ""
            return True, f"Set asset_tag='{asset_tag}'{extra}"
        else:
            return False, f"HTTP {resp.status_code}: {resp.text[:200]}"
    except Exception as e:
        return False, f"Request failed: {e}"


def run_netbox_updates(matches, dry_run):
    """Phase 3: Update asset_tags in NetBox for matched entries."""
    print(f"\n{'='*80}")
    print("Phase 3: NetBox asset tag updates")
    print(f"{'='*80}\n")

    successes = 0
    failures = 0
    already_set = 0

    for m in sorted(matches, key=lambda e: e.get("hostname", "")):
        nb_dev = m["nb_device"]
        csv_tag = m["asset_tag"]
        current_tag = nb_dev["asset_tag"]
        label = m["hostname"] or m["full_hostname"]
        nb_name = nb_dev["name"]

        if current_tag == csv_tag:
            already_set += 1
            print(f"  [SKIP] {label:<45} tag={csv_tag:<6} "
                  f"NB='{nb_name}' — Already set")
            continue

        ok, msg = update_device_asset_tag(
            nb_dev["id"], nb_name, csv_tag, nb_dev["owner"], dry_run,
        )
        if ok:
            successes += 1
            status = "OK  " if not dry_run else "DRY "
        else:
            failures += 1
            status = "FAIL"

        current_str = f"'{current_tag}'" if current_tag else "none"
        print(f"  [{status}] {label:<45} tag={csv_tag:<6} "
              f"NB='{nb_name}' (was {current_str}) {msg}")

    total = successes + failures + already_set
    print(f"\nResults: {successes} updated, {already_set} already set, "
          f"{failures} failed (of {total} matched)")
    return failures == 0


# ── Phase 4: Post-update verification ────────────────────────────────────────

def run_post_verification(nb, matches):
    """Re-read each updated device by ID and confirm asset_tag matches."""
    print(f"\n{'='*80}")
    print("Phase 4: Post-update verification")
    print(f"{'='*80}\n")

    ok_count = 0
    fail_count = 0

    for m in sorted(matches, key=lambda e: e.get("hostname", "")):
        expected_tag = m["asset_tag"]
        nb_dev = m["nb_device"]
        label = m["hostname"] or m["full_hostname"]

        device = nb.dcim.devices.get(nb_dev["id"])
        if not device:
            print(f"  FAIL  {label:<45} device id={nb_dev['id']} not found")
            fail_count += 1
            continue

        actual_tag = device.asset_tag or ""
        if actual_tag == expected_tag:
            print(f"  OK    {label:<45} asset_tag={actual_tag}")
            ok_count += 1
        else:
            print(f"  FAIL  {label:<45} expected='{expected_tag}' got='{actual_tag}'")
            fail_count += 1

    print(f"\nVerification: {ok_count} OK, {fail_count} failed "
          f"(of {len(matches)} total)")
    return fail_count == 0


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Sync compute asset tags from CSV into NetBox.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  %(prog)s --dry-run                    # preview all changes
  %(prog)s                              # execute updates
  %(prog)s --filter potato --dry-run    # preview potato nodes only
  %(prog)s --filter potato              # program potato nodes only
  %(prog)s --hostnames-csv /custom.csv  # custom CSV path
""")
    parser.add_argument("--dry-run", action="store_true",
                        help="show what would change, don't write to NetBox")
    parser.add_argument("--filter",
                        help="only process entries whose friendly name, hostname, "
                             "or full hostname contains this substring (case-insensitive)")
    parser.add_argument("--hostnames-csv", default=HOSTNAMES_CSV,
                        help=f"path to hostnames.csv (default: {HOSTNAMES_CSV})")
    args = parser.parse_args()

    if args.dry_run:
        print("=== DRY RUN — no changes will be made ===\n")

    # ── Phase 1 ──────────────────────────────────────────────────────────
    print(f"{'='*80}")
    print("Phase 1: Loading CSV data")
    print(f"{'='*80}")
    print(f"  hostnames.csv: {args.hostnames_csv}")

    csv_entries, skip_count = load_hostnames_csv(args.hostnames_csv)

    print(f"\n  Loaded:  {len(csv_entries)} entries with identifiers")
    print(f"  Skipped: {skip_count} rows (no asset tag or no identifiers)")

    # Apply filter if specified
    if args.filter:
        pattern = args.filter.lower()
        csv_entries = [
            e for e in csv_entries
            if pattern in (e["hostname"] or "").lower()
            or pattern in (e["full_hostname"] or "").lower()
            or any(pattern in v for v in e["hostname_variants"])
        ]
        print(f"  Filter:  '{args.filter}' → {len(csv_entries)} entries remaining")

    if not csv_entries:
        print("No entries to process.")
        return

    # ── Phase 2 ──────────────────────────────────────────────────────────
    print(f"\n{'='*80}")
    print("Phase 2: Matching CSV entries to NetBox devices")
    print(f"{'='*80}")

    nb = get_api()
    print(f"\n  Loading devices from roles: {', '.join(DEVICE_ROLES)}...")
    nb_devices = load_netbox_devices(nb)
    print(f"  Loaded {len(nb_devices)} NetBox devices")

    mac_index, serial_index, name_index = build_match_indexes(nb_devices)
    print(f"  Indexes: {len(mac_index)} MACs, {len(serial_index)} serials, "
          f"{len(name_index)} names")

    matches, unmatched = match_csv_to_netbox(csv_entries, mac_index,
                                             serial_index, name_index)

    # Report match methods
    method_counts = {}
    for m in matches:
        method = m["match_method"]
        method_counts[method] = method_counts.get(method, 0) + 1

    print(f"\n  Matched:   {len(matches)} entries")
    for method, count in sorted(method_counts.items()):
        print(f"    {method}: {count}")
    print(f"  Unmatched: {len(unmatched)} entries")

    if unmatched:
        print("\n  Unmatched entries:")
        for entry in unmatched:
            label = entry["hostname"] or entry["full_hostname"]
            macs_str = ", ".join(entry["macs"][:1]) if entry["macs"] else "none"
            print(f"    {label:<45} tag={entry['asset_tag']:<6} "
                  f"mac={macs_str}")

    if not matches:
        print("\nNo matches found. Nothing to update.")
        return

    # ── Phase 3 ──────────────────────────────────────────────────────────
    all_ok = run_netbox_updates(matches, args.dry_run)

    if args.dry_run:
        print("\n=== DRY RUN complete — re-run without --dry-run to execute ===")
        return

    if not all_ok:
        print("\nSome updates failed. Review output above.")
        sys.exit(1)

    # ── Phase 4 ──────────────────────────────────────────────────────────
    run_post_verification(nb, matches)


if __name__ == "__main__":
    main()

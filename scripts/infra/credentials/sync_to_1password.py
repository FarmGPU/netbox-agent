#!/usr/bin/env python3
"""
Sync infrastructure credentials from validated CSVs into 1Password.

Creates up to 6 Secure Note items (3 domains x 2 credential types) in the
specified vault.  Only devices with asset tags are included.  Each device
gets its own section keyed by asset tag with:
  - Username  [text]
  - Password  [concealed]   <- eye-icon toggle in 1Password UI

All other device metadata (IP, hostname, protocol, interfaces) lives in
NetBox as the source of truth -- 1Password is strictly for passwords.

Domains:
  pdu          ServerTech PDU management interfaces
  compute-bmc  Server BMC/IPMI/Redfish interfaces
  switches     Network switches (Arista eAPI, etc.)

Credential types:
  default      Factory/default credentials (admn, ADMIN, etc.)
  farmgpu      Operational FarmGPU credentials

Usage:
  # Preview what would be created (no 1Password access needed)
  python3 sync_to_1password.py --dry-run

  # Create/update items in 1Password (must be signed in: op signin)
  python3 sync_to_1password.py --vault='Employee'

  # Replace existing items with fresh data
  python3 sync_to_1password.py --vault='Employee' --update

Requirements:
  - 1Password CLI v2 (`op`) installed and signed in
  - Validated CSV files from asset-tag-testing/csvs/
"""

import argparse
import csv
import json
import os
import subprocess
import sys

# ── CSV paths ────────────────────────────────────────────────────────────────
CSV_BASE = os.path.join(os.path.expanduser("~"), "asset-tag-testing", "csvs")
PDU_CSV = os.path.join(CSV_BASE, "pdus", "validated_hosts.csv")
COMPUTE_CSV = os.path.join(CSV_BASE, "compute", "validated_hosts.csv")
SWITCHES_CSV = os.path.join(CSV_BASE, "switches", "arista_switches.csv")
MACHINES_CSV = os.path.join(CSV_BASE, "compute", "machines_validated.csv")

# ── Credential classification ────────────────────────────────────────────────
# Usernames considered "default/factory" credentials
DEFAULT_USERS = {"admn", "admin", "ADMIN", "root", "USERID"}

VAULT_DEFAULT = "Employee"

# Item title template
TITLES = {
    ("pdu", "default"):          "PDU — Default Credentials",
    ("pdu", "farmgpu"):          "PDU — FarmGPU Credentials",
    ("compute-bmc", "default"):  "Compute BMC — Default Credentials",
    ("compute-bmc", "farmgpu"):  "Compute BMC — FarmGPU Credentials",
    ("switches", "default"):     "Switches — Default Credentials",
    ("switches", "farmgpu"):     "Switches — FarmGPU Credentials",
}


# ── CSV loading ──────────────────────────────────────────────────────────────
# Each loader returns list of (asset_tag, user, password) tuples.
# Only entries WITH an asset_tag are included.

def load_pdu_credentials(path):
    """Load PDU credentials for entries with asset tags."""
    entries = []
    if not os.path.exists(path):
        return entries
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            asset_tag = row.get("asset_tag", "").strip()
            if not asset_tag:
                continue
            user = row.get("validated_user", "").strip()
            pw = row.get("validated_password", "").strip()
            if not user:
                continue
            entries.append((asset_tag, user, pw))
    return entries


def load_compute_credentials(path):
    """Load compute BMC credentials for entries with asset tags."""
    entries = []
    if not os.path.exists(path):
        return entries
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            asset_tag = row.get("asset_tag", "").strip()
            if not asset_tag:
                continue
            user = row.get("validated_user", "").strip()
            pw = row.get("validated_password", "").strip()
            if not user:
                continue
            entries.append((asset_tag, user, pw))
    return entries


def load_switch_credentials(path):
    """Load switch credentials for entries with asset tags."""
    entries = []
    if not os.path.exists(path):
        return entries
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            asset_tag = row.get("asset_tag", "").strip()
            if not asset_tag:
                continue
            # Switches may not have credentials discovered yet
            user = row.get("validated_user", "").strip()
            pw = row.get("validated_password", "").strip()
            if not user:
                continue
            entries.append((asset_tag, user, pw))
    return entries


def load_machines_credentials(path):
    """Load OEM BMC credentials from machines_validated.csv.

    Produced by validate_machines_csv.py — contains OEM/admin default
    passwords from machines.csv, cross-referenced with hostnames.csv
    for asset tags and validated against live BMCs.

    All entries with an asset_tag are included, regardless of validation
    status.  Passwords are always present (from the source CSV); the
    status field indicates whether the credential was live-verified.
    """
    entries = []
    if not os.path.exists(path):
        return entries
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            asset_tag = row.get("asset_tag", "").strip()
            if not asset_tag:
                continue
            user = row.get("validated_user", "").strip()
            pw = row.get("validated_password", "").strip()
            if not user or not pw:
                continue
            entries.append((asset_tag, user, pw))
    return entries


def classify_credentials(entries):
    """
    Split entries into default vs farmgpu buckets.
    Returns (default_list, farmgpu_list).
    """
    default_entries = []
    farmgpu_entries = []
    for asset_tag, user, pw in entries:
        if user.lower() in {u.lower() for u in DEFAULT_USERS}:
            default_entries.append((asset_tag, user, pw))
        else:
            farmgpu_entries.append((asset_tag, user, pw))
    return default_entries, farmgpu_entries


# ── 1Password item generation ────────────────────────────────────────────────

def escape_op_field(s):
    """Escape periods, equals signs, and backslashes for op field names."""
    s = s.replace("\\", "\\\\")
    s = s.replace(".", "\\.")
    s = s.replace("=", "\\=")
    return s


def build_op_fields(entries):
    """
    Build op CLI field assignment strings for a list of credential entries.

    Each device gets its own section keyed by asset tag.
    Only Username and Password are stored -- everything else is in NetBox.
    Password fields use [concealed] type for eye-icon toggle in 1Password UI.
    """
    fields = []
    seen_tags = set()

    for asset_tag, user, pw in sorted(entries, key=lambda e: e[0]):
        if asset_tag in seen_tags:
            continue
        seen_tags.add(asset_tag)

        section = escape_op_field(asset_tag)
        fields.append(f"{section}.Username[text]={user}")
        if pw:
            fields.append(f"{section}.Password[concealed]={pw}")
        else:
            fields.append(f"{section}.Password[concealed]=(not yet discovered)")

    return fields


def build_summary_note(domain, cred_type, entries):
    """Build a summary note for the item's notesPlain field."""
    with_creds = sum(1 for _, u, p in entries if u and p)
    unique_users = set(u for _, u, _ in entries if u)
    unique_tags = set(t for t, _, _ in entries)
    return (
        f"Domain: {domain}\n"
        f"Credential type: {cred_type}\n"
        f"Total devices: {len(unique_tags)}\n"
        f"With credentials: {with_creds}\n"
        f"Unique usernames: {', '.join(sorted(unique_users)) or '(none)'}\n"
        f"\nIndexed by asset tag. All other metadata (IP, hostname,\n"
        f"protocol) lives in NetBox as the source of truth.\n"
        f"\nGenerated by sync_to_1password.py\n"
    )


def create_or_update_item(title, vault, domain, cred_type, entries, dry_run=False, update=False):
    """Create or update a 1Password Secure Note item."""
    fields = build_op_fields(entries)
    summary = build_summary_note(domain, cred_type, entries)

    if not entries:
        print(f"\n  [{title}] — No entries, skipping.")
        return True

    unique_tags = len(set(t for t, _, _ in entries))
    print(f"\n  [{title}]")
    print(f"    Devices (by asset tag): {unique_tags}")
    print(f"    Fields: {len(fields)}")

    if dry_run:
        # Show a sample of fields (mask passwords)
        for f in fields[:20]:
            if "[concealed]" in f:
                key, _, _ = f.partition("=")
                print(f"    {key}=********")
            else:
                print(f"    {f}")
        if len(fields) > 20:
            print(f"    ... and {len(fields) - 20} more fields")
        return True

    # Build op command
    cmd = [
        "op", "item", "create",
        "--category=Secure Note",
        f"--vault={vault}",
        f"--title={title}",
        f"notesPlain={summary}",
    ] + fields

    if update:
        # Find and delete existing item first (op doesn't support bulk field updates)
        find_cmd = ["op", "item", "get", title, f"--vault={vault}", "--format=json"]
        result = subprocess.run(find_cmd, capture_output=True, text=True)
        if result.returncode == 0:
            item_id = json.loads(result.stdout).get("id", "")
            if item_id:
                subprocess.run(["op", "item", "delete", item_id, f"--vault={vault}"],
                               capture_output=True)
                print(f"    Deleted existing item {item_id} for recreation.")

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        item_data = json.loads(result.stdout) if result.stdout.strip().startswith("{") else {}
        item_id = item_data.get("id", "created")
        print(f"    Created: {item_id}")
        return True
    else:
        print(f"    FAILED: {result.stderr.strip()}")
        return False


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Sync infrastructure credentials to 1Password",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  %(prog)s --dry-run                    # preview (no op signin needed)
  %(prog)s --vault=Employee             # create items
  %(prog)s --vault=Employee --update    # replace existing items
""")
    parser.add_argument("--dry-run", action="store_true",
                        help="preview items without creating them")
    parser.add_argument("--vault", default=VAULT_DEFAULT,
                        help=f"1Password vault name (default: {VAULT_DEFAULT})")
    parser.add_argument("--update", action="store_true",
                        help="delete and recreate existing items")
    parser.add_argument("--pdu-csv", default=PDU_CSV)
    parser.add_argument("--compute-csv", default=COMPUTE_CSV)
    parser.add_argument("--machines-csv", default=MACHINES_CSV,
                        help="OEM BMC credentials from machines_validated.csv")
    parser.add_argument("--switches-csv", default=SWITCHES_CSV)
    args = parser.parse_args()

    if args.dry_run:
        print("=== DRY RUN — no 1Password changes ===\n")
    else:
        # Verify op is signed in
        result = subprocess.run(["op", "vault", "list", "--format=json"],
                                capture_output=True, text=True)
        if result.returncode != 0:
            print("ERROR: Not signed in to 1Password. Run: op signin")
            sys.exit(1)
        vaults = json.loads(result.stdout)
        vault_names = [v["name"] for v in vaults]
        if args.vault not in vault_names:
            print(f"ERROR: Vault '{args.vault}' not found. Available: {vault_names}")
            sys.exit(1)

    print(f"Vault: {args.vault}")
    print(f"CSVs:  pdu={args.pdu_csv}")
    print(f"       compute={args.compute_csv}")
    print(f"       machines={args.machines_csv}")
    print(f"       switches={args.switches_csv}")

    # Load all credentials (only entries with asset tags)
    pdu_all = load_pdu_credentials(args.pdu_csv)
    compute_all = load_compute_credentials(args.compute_csv)
    machines_all = load_machines_credentials(args.machines_csv)
    switch_all = load_switch_credentials(args.switches_csv)

    print(f"\nLoaded: {len(pdu_all)} PDU, {len(compute_all)} compute, "
          f"{len(machines_all)} machines OEM, {len(switch_all)} switch credentials (with asset tags)")

    # Merge machines OEM creds into compute — these are all ADMIN/default.
    # Deduplicate by asset_tag: if the same tag exists in both compute_all
    # and machines_all, keep both (they may be different users: fgpu vs ADMIN).
    compute_all = compute_all + machines_all

    # Classify into default vs farmgpu
    pdu_default, pdu_farmgpu = classify_credentials(pdu_all)
    compute_default, compute_farmgpu = classify_credentials(compute_all)
    switch_default, switch_farmgpu = classify_credentials(switch_all)

    items = [
        ("pdu", "default", pdu_default),
        ("pdu", "farmgpu", pdu_farmgpu),
        ("compute-bmc", "default", compute_default),
        ("compute-bmc", "farmgpu", compute_farmgpu),
        ("switches", "default", switch_default),
        ("switches", "farmgpu", switch_farmgpu),
    ]

    print(f"\n{'='*60}")
    print("Items to create:")
    print(f"{'='*60}")

    all_ok = True
    for domain, cred_type, entries in items:
        title = TITLES[(domain, cred_type)]
        ok = create_or_update_item(
            title, args.vault, domain, cred_type, entries,
            dry_run=args.dry_run, update=args.update,
        )
        if not ok:
            all_ok = False

    if args.dry_run:
        print(f"\n=== DRY RUN complete ===")
    else:
        status = "All items created." if all_ok else "Some items failed."
        print(f"\n{status}")

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())

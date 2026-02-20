#!/usr/bin/env python3
"""
Push infrastructure credentials to 1Password as individual Login items.

Structure:
  ethan-infra-fgpu  — All non-default credentials (fgpu, jmhands, etc.)
  ethan-infra-oem   — All default/factory credentials (ADMIN, admin, admn, root, USERID)

Each item:
  Title:    {asset_tag}
  Category: Login
  Username: the validated username
  Password: the validated password

All device types (compute, PDU, switches) are mixed in the same vaults.
One item per device per vault. If a device has both an OEM and fgpu
credential, it gets one item in each vault.

Usage:
  python3 push_to_vaults.py --dry-run           # preview
  python3 push_to_vaults.py                      # execute
  python3 push_to_vaults.py --update             # delete + recreate existing
"""

import argparse
import csv
import json
import os
import subprocess
import sys

# ── Vault names ──────────────────────────────────────────────────────────────
VAULT_FGPU = "ethan-infra-fgpu"
VAULT_OEM = "ethan-infra-oem"

# ── CSV paths ────────────────────────────────────────────────────────────────
CSV_BASE = os.path.join(os.path.expanduser("~"), "asset-tag-testing", "csvs")
PDU_CSV = os.path.join(CSV_BASE, "pdus", "validated_hosts.csv")
COMPUTE_CSV = os.path.join(CSV_BASE, "compute", "validated_hosts.csv")
MACHINES_CSV = os.path.join(CSV_BASE, "compute", "machines_validated.csv")
SWITCHES_CSV = os.path.join(CSV_BASE, "switches", "arista_switches.csv")

# ── Credential classification ────────────────────────────────────────────────
DEFAULT_USERS = {"admn", "admin", "ADMIN", "root", "USERID"}


def load_csv_credentials(path):
    """Load (asset_tag, user, password) tuples from a validated CSV."""
    entries = []
    if not os.path.exists(path):
        return entries
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            tag = row.get("asset_tag", "").strip()
            if not tag:
                continue
            user = row.get("validated_user", "").strip()
            pw = row.get("validated_password", "").strip()
            if not user or not pw:
                continue
            entries.append((tag, user, pw))
    return entries


def is_default(username):
    """Check if a username is a factory/OEM default."""
    return username.lower() in {u.lower() for u in DEFAULT_USERS}


def create_login_item(vault, title, username, password, dry_run=False, update=False):
    """Create a single Login item in 1Password."""
    if dry_run:
        vault_short = "oem" if "oem" in vault else "fgpu"
        print(f"  [{vault_short}] {title:<8} user={username}")
        return True

    if update:
        # Check if item exists and delete it
        result = subprocess.run(
            ["op", "item", "get", title, f"--vault={vault}", "--format=json"],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            try:
                item_id = json.loads(result.stdout).get("id", "")
                if item_id:
                    subprocess.run(
                        ["op", "item", "delete", item_id, f"--vault={vault}"],
                        capture_output=True,
                    )
            except json.JSONDecodeError:
                pass

    result = subprocess.run(
        [
            "op", "item", "create",
            "--category=Login",
            f"--vault={vault}",
            f"--title={title}",
            f"username={username}",
            f"password={password}",
        ],
        capture_output=True, text=True,
    )

    if result.returncode == 0:
        return True
    else:
        err = result.stderr.strip()
        if "already exists" in err.lower():
            print(f"  SKIP {title} — already exists in {vault}")
            return True
        print(f"  FAIL {title} — {err}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Push credentials to 1Password vaults (individual items per device)",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without creating items")
    parser.add_argument("--update", action="store_true",
                        help="Delete and recreate existing items")
    parser.add_argument("--fgpu-vault", default=VAULT_FGPU)
    parser.add_argument("--oem-vault", default=VAULT_OEM)
    args = parser.parse_args()

    if not args.dry_run:
        # Verify signed in and vaults exist
        result = subprocess.run(
            ["op", "vault", "list", "--format=json"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            print("ERROR: Not signed in to 1Password. Run: eval $(op signin)")
            sys.exit(1)
        vaults = {v["name"] for v in json.loads(result.stdout)}
        for v in [args.fgpu_vault, args.oem_vault]:
            if v not in vaults:
                print(f"ERROR: Vault '{v}' not found. Available: {sorted(vaults)}")
                sys.exit(1)

    # Load all credential sources
    pdu = load_csv_credentials(PDU_CSV)
    compute = load_csv_credentials(COMPUTE_CSV)
    machines = load_csv_credentials(MACHINES_CSV)
    switches = load_csv_credentials(SWITCHES_CSV)

    all_creds = pdu + compute + machines + switches
    print(f"Loaded: {len(pdu)} PDU + {len(compute)} compute + {len(machines)} machines OEM + {len(switches)} switch = {len(all_creds)} total")

    # Classify and deduplicate: one entry per (tag, vault)
    # If same tag has multiple creds for same vault, keep first
    fgpu_items = {}  # tag -> (user, pw)
    oem_items = {}   # tag -> (user, pw)

    for tag, user, pw in all_creds:
        if is_default(user):
            if tag not in oem_items:
                oem_items[tag] = (user, pw)
        else:
            if tag not in fgpu_items:
                fgpu_items[tag] = (user, pw)

    print(f"\nItems to create:")
    print(f"  {args.fgpu_vault}: {len(fgpu_items)} devices")
    print(f"  {args.oem_vault}:  {len(oem_items)} devices")

    if args.dry_run:
        print(f"\n=== DRY RUN ===\n")

    # Create OEM items
    print(f"\n── {args.oem_vault} ({len(oem_items)} items) ──")
    oem_ok = 0
    oem_fail = 0
    for tag in sorted(oem_items):
        user, pw = oem_items[tag]
        if create_login_item(args.oem_vault, tag, user, pw,
                             dry_run=args.dry_run, update=args.update):
            oem_ok += 1
        else:
            oem_fail += 1

    # Create FGPU items
    print(f"\n── {args.fgpu_vault} ({len(fgpu_items)} items) ──")
    fgpu_ok = 0
    fgpu_fail = 0
    for tag in sorted(fgpu_items):
        user, pw = fgpu_items[tag]
        if create_login_item(args.fgpu_vault, tag, user, pw,
                             dry_run=args.dry_run, update=args.update):
            fgpu_ok += 1
        else:
            fgpu_fail += 1

    print(f"\n{'='*50}")
    if args.dry_run:
        print(f"DRY RUN complete.")
    else:
        print(f"{args.oem_vault}:  {oem_ok} created, {oem_fail} failed")
        print(f"{args.fgpu_vault}: {fgpu_ok} created, {fgpu_fail} failed")

    return 0 if (oem_fail == 0 and fgpu_fail == 0) else 1


if __name__ == "__main__":
    sys.exit(main())

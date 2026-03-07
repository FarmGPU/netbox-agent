#!/usr/bin/env python3
"""
Migrate IPAM data from old NetBox (netbox.farmgpu.net) to new NetBox (10.100.248.18).

Migrates: Tags, Tenant Groups, Tenants, RIRs, Aggregates, IPAM Roles,
          VLANs, VRFs, Route Targets, Prefixes, IP Ranges.

Does NOT migrate: IP Addresses, Devices, Interfaces, Cables, Racks, Sites.

Usage:
    python scripts/migration/migrate_ipam.py --dry-run
    python scripts/migration/migrate_ipam.py
    python scripts/migration/migrate_ipam.py --verify-only
    python scripts/migration/migrate_ipam.py --phase 9
"""

import argparse
import ipaddress
import logging
import urllib3

import pynetbox

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SRC_URL = "https://netbox.farmgpu.net"
SRC_TOKEN = "52f935016d33f0ad635b23c627adb62a07430558"
DST_URL = "https://10.100.248.18"
DST_TOKEN = "nbt_iKGuMp3OpEse.8O1M1A2PAgUJduwIhnpDZnAmgDxpRT9DVvftSj6o"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class IdMapper:
    """Track old_id → new_id mappings across migration phases."""

    def __init__(self):
        self._maps = {}

    def set(self, category, old_id, new_id):
        self._maps.setdefault(category, {})[old_id] = new_id

    def get(self, category, old_id):
        if old_id is None:
            return None
        return self._maps.get(category, {}).get(old_id)


class PhaseStats:
    """Per-phase counters for created / updated / skipped / failed."""

    def __init__(self, name):
        self.name = name
        self.created = 0
        self.updated = 0
        self.skipped = 0
        self.failed = 0

    def print_summary(self):
        total = self.created + self.updated + self.skipped + self.failed
        print(
            f"  {self.name}: {total} processed"
            f" — {self.created} created, {self.updated} updated,"
            f" {self.skipped} skipped, {self.failed} failed"
        )


def _status_val(obj):
    """Extract status string (e.g. 'active') from a pynetbox status field."""
    if hasattr(obj, "value"):
        return obj.value
    return obj


def _obj_id(obj):
    """Extract numeric ID from a nested pynetbox object or dict."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get("id")
    return getattr(obj, "id", None)


def _map_tags(src_tags, mapper):
    """Convert a source tag list to destination tag ID dicts."""
    if not src_tags:
        return []
    mapped = []
    for tag in src_tags:
        tag_id = tag["id"] if isinstance(tag, dict) else tag.id
        new_id = mapper.get("tags", tag_id)
        if new_id is not None:
            mapped.append({"id": new_id})
    return mapped


def _mapped_fk(mapper, category, src_obj):
    """Return mapped destination ID for a foreign-key field, or None."""
    old_id = _obj_id(src_obj)
    if old_id is None:
        return None
    return mapper.get(category, old_id)


def connect():
    """Establish connections to both NetBox instances."""
    src = pynetbox.api(SRC_URL, token=SRC_TOKEN)
    dst = pynetbox.api(DST_URL, token=DST_TOKEN)
    dst.http_session.verify = False
    logger.info("Source:      %s (v%s)", SRC_URL, src.version)
    logger.info("Destination: %s (v%s)", DST_URL, dst.version)
    return src, dst


# ---------------------------------------------------------------------------
# Phase 1: Tags
# ---------------------------------------------------------------------------

def migrate_tags(src, dst, mapper, dry_run):
    stats = PhaseStats("Tags")
    print("\n" + "=" * 60)
    print("Phase 1: Tags")
    print("=" * 60)

    src_tags = list(src.extras.tags.all())
    print(f"  Source: {len(src_tags)}")

    for tag in src_tags:
        try:
            existing = dst.extras.tags.get(slug=tag.slug)
            if existing:
                mapper.set("tags", tag.id, existing.id)
                stats.skipped += 1
                logger.info("Tag '%s' exists (id=%d), skipped", tag.slug, existing.id)
                continue

            if dry_run:
                logger.info("[DRY RUN] Would create tag '%s'", tag.name)
                stats.created += 1
                continue

            new = dst.extras.tags.create(
                name=tag.name,
                slug=tag.slug,
                color=tag.color.lower(),
                description=tag.description or "",
            )
            mapper.set("tags", tag.id, new.id)
            stats.created += 1
            logger.info("Created tag '%s' (id=%d)", tag.name, new.id)
        except Exception as e:
            stats.failed += 1
            logger.error("Failed tag '%s': %s", tag.name, e)

    stats.print_summary()
    return stats


# ---------------------------------------------------------------------------
# Phase 2: Tenant Groups
# ---------------------------------------------------------------------------

def migrate_tenant_groups(src, dst, mapper, dry_run):
    stats = PhaseStats("Tenant Groups")
    print("\n" + "=" * 60)
    print("Phase 2: Tenant Groups")
    print("=" * 60)

    src_groups = list(src.tenancy.tenant_groups.all())
    print(f"  Source: {len(src_groups)}")

    for grp in src_groups:
        try:
            existing = dst.tenancy.tenant_groups.get(slug=grp.slug)
            if existing:
                mapper.set("tenant_groups", grp.id, existing.id)
                stats.skipped += 1
                logger.info("Tenant group '%s' exists, skipped", grp.slug)
                continue

            if dry_run:
                logger.info("[DRY RUN] Would create tenant group '%s'", grp.name)
                stats.created += 1
                continue

            new = dst.tenancy.tenant_groups.create(
                name=grp.name,
                slug=grp.slug,
                description=grp.description or "",
            )
            mapper.set("tenant_groups", grp.id, new.id)
            stats.created += 1
            logger.info("Created tenant group '%s' (id=%d)", grp.name, new.id)
        except Exception as e:
            stats.failed += 1
            logger.error("Failed tenant group '%s': %s", grp.name, e)

    stats.print_summary()
    return stats


# ---------------------------------------------------------------------------
# Phase 3: Tenants
# ---------------------------------------------------------------------------

def migrate_tenants(src, dst, mapper, dry_run):
    stats = PhaseStats("Tenants")
    print("\n" + "=" * 60)
    print("Phase 3: Tenants")
    print("=" * 60)

    src_tenants = list(src.tenancy.tenants.all())
    print(f"  Source: {len(src_tenants)}")

    for tenant in src_tenants:
        try:
            existing = dst.tenancy.tenants.get(slug=tenant.slug)
            if existing:
                mapper.set("tenants", tenant.id, existing.id)
                stats.skipped += 1
                logger.info("Tenant '%s' exists, skipped", tenant.slug)
                continue

            if dry_run:
                logger.info("[DRY RUN] Would create tenant '%s'", tenant.name)
                stats.created += 1
                continue

            data = {
                "name": tenant.name,
                "slug": tenant.slug,
                "description": tenant.description or "",
                "tags": _map_tags(tenant.tags, mapper),
            }
            mapped_grp = _mapped_fk(mapper, "tenant_groups", tenant.group)
            if mapped_grp:
                data["group"] = mapped_grp

            new = dst.tenancy.tenants.create(data)
            mapper.set("tenants", tenant.id, new.id)
            stats.created += 1
            logger.info("Created tenant '%s' (id=%d)", tenant.name, new.id)
        except Exception as e:
            stats.failed += 1
            logger.error("Failed tenant '%s': %s", tenant.name, e)

    stats.print_summary()
    return stats


# ---------------------------------------------------------------------------
# Phase 4: RIRs
# ---------------------------------------------------------------------------

def migrate_rirs(src, dst, mapper, dry_run):
    stats = PhaseStats("RIRs")
    print("\n" + "=" * 60)
    print("Phase 4: RIRs")
    print("=" * 60)

    src_rirs = list(src.ipam.rirs.all())
    print(f"  Source: {len(src_rirs)}")

    for rir in src_rirs:
        try:
            existing = dst.ipam.rirs.get(slug=rir.slug)
            if existing:
                mapper.set("rirs", rir.id, existing.id)
                stats.skipped += 1
                logger.info("RIR '%s' exists, skipped", rir.slug)
                continue

            if dry_run:
                logger.info("[DRY RUN] Would create RIR '%s'", rir.name)
                stats.created += 1
                continue

            new = dst.ipam.rirs.create(
                name=rir.name,
                slug=rir.slug,
                is_private=rir.is_private,
                description=rir.description or "",
            )
            mapper.set("rirs", rir.id, new.id)
            stats.created += 1
            logger.info("Created RIR '%s' (id=%d)", rir.name, new.id)
        except Exception as e:
            stats.failed += 1
            logger.error("Failed RIR '%s': %s", rir.name, e)

    stats.print_summary()
    return stats


# ---------------------------------------------------------------------------
# Phase 5: Aggregates
# ---------------------------------------------------------------------------

def migrate_aggregates(src, dst, mapper, dry_run):
    stats = PhaseStats("Aggregates")
    print("\n" + "=" * 60)
    print("Phase 5: Aggregates")
    print("=" * 60)

    src_aggs = list(src.ipam.aggregates.all())
    print(f"  Source: {len(src_aggs)}")

    for agg in src_aggs:
        try:
            existing = list(dst.ipam.aggregates.filter(prefix=agg.prefix))
            if existing:
                stats.skipped += 1
                logger.info("Aggregate '%s' exists, skipped", agg.prefix)
                continue

            if dry_run:
                logger.info("[DRY RUN] Would create aggregate '%s'", agg.prefix)
                stats.created += 1
                continue

            data = {
                "prefix": agg.prefix,
                "description": agg.description or "",
                "tags": _map_tags(agg.tags, mapper),
            }
            mapped_rir = _mapped_fk(mapper, "rirs", agg.rir)
            if mapped_rir:
                data["rir"] = mapped_rir

            dst.ipam.aggregates.create(data)
            stats.created += 1
            logger.info("Created aggregate '%s'", agg.prefix)
        except Exception as e:
            stats.failed += 1
            logger.error("Failed aggregate '%s': %s", agg.prefix, e)

    stats.print_summary()
    return stats


# ---------------------------------------------------------------------------
# Phase 6: IPAM Roles
# ---------------------------------------------------------------------------

def migrate_roles(src, dst, mapper, dry_run):
    stats = PhaseStats("IPAM Roles")
    print("\n" + "=" * 60)
    print("Phase 6: IPAM Roles")
    print("=" * 60)

    src_roles = list(src.ipam.roles.all())
    print(f"  Source: {len(src_roles)}")

    for role in src_roles:
        try:
            existing = dst.ipam.roles.get(slug=role.slug)
            if existing:
                mapper.set("roles", role.id, existing.id)
                stats.skipped += 1
                logger.info("Role '%s' exists, skipped", role.slug)
                continue

            if dry_run:
                logger.info("[DRY RUN] Would create role '%s'", role.name)
                stats.created += 1
                continue

            new = dst.ipam.roles.create(
                name=role.name,
                slug=role.slug,
                weight=role.weight,
                description=role.description or "",
            )
            mapper.set("roles", role.id, new.id)
            stats.created += 1
            logger.info("Created role '%s' (id=%d)", role.name, new.id)
        except Exception as e:
            stats.failed += 1
            logger.error("Failed role '%s': %s", role.name, e)

    stats.print_summary()
    return stats


# ---------------------------------------------------------------------------
# Phase 7: VLANs  (101 source → merge with 4 existing bare VLANs)
# ---------------------------------------------------------------------------

def migrate_vlans(src, dst, mapper, dry_run):
    stats = PhaseStats("VLANs")
    print("\n" + "=" * 60)
    print("Phase 7: VLANs")
    print("=" * 60)

    src_vlans = list(src.ipam.vlans.all())
    print(f"  Source: {len(src_vlans)}")

    # Snapshot existing dest VLANs BEFORE any mutations, grouped by VID.
    # Each candidate can only be claimed once (handles duplicate VID 300).
    dst_pool = {}
    for v in dst.ipam.vlans.all():
        dst_pool.setdefault(v.vid, []).append(v)
    print(f"  Existing in dest: {sum(len(v) for v in dst_pool.values())}")

    for vlan in src_vlans:
        try:
            candidates = dst_pool.get(vlan.vid, [])
            match = candidates[0] if candidates else None

            if match:
                # Claim this candidate so a second source VLAN with the same
                # VID (e.g. VID 300) won't re-match it.
                candidates.remove(match)

                if not match.description:
                    # Bare VLAN — update with source data
                    if dry_run:
                        logger.info(
                            "[DRY RUN] Would update VLAN %d '%s' with source data",
                            vlan.vid, vlan.name,
                        )
                    else:
                        match.name = vlan.name
                        match.description = vlan.description or ""
                        match.status = _status_val(vlan.status)
                        mapped_tenant = _mapped_fk(mapper, "tenants", vlan.tenant)
                        if mapped_tenant:
                            match.tenant = mapped_tenant
                        match.tags = _map_tags(vlan.tags, mapper)
                        match.save()
                        logger.info("Updated VLAN %d '%s'", vlan.vid, vlan.name)
                    mapper.set("vlans", vlan.id, match.id)
                    stats.updated += 1
                else:
                    # Already has description — skip
                    mapper.set("vlans", vlan.id, match.id)
                    stats.skipped += 1
                    logger.info("VLAN %d already complete, skipped", vlan.vid)
                continue

            # No existing match — create
            if dry_run:
                logger.info("[DRY RUN] Would create VLAN %d '%s'", vlan.vid, vlan.name)
                stats.created += 1
                continue

            data = {
                "vid": vlan.vid,
                "name": vlan.name,
                "description": vlan.description or "",
                "status": _status_val(vlan.status),
                "tags": _map_tags(vlan.tags, mapper),
            }
            mapped_tenant = _mapped_fk(mapper, "tenants", vlan.tenant)
            if mapped_tenant:
                data["tenant"] = mapped_tenant

            new = dst.ipam.vlans.create(data)
            mapper.set("vlans", vlan.id, new.id)
            stats.created += 1
            logger.info("Created VLAN %d '%s' (id=%d)", vlan.vid, vlan.name, new.id)
        except Exception as e:
            stats.failed += 1
            logger.error("Failed VLAN %d '%s': %s", vlan.vid, vlan.name, e)

    stats.print_summary()
    return stats


# ---------------------------------------------------------------------------
# Phase 8: VRFs + Route Targets
# ---------------------------------------------------------------------------

def migrate_vrfs(src, dst, mapper, dry_run):
    stats = PhaseStats("VRFs")
    print("\n" + "=" * 60)
    print("Phase 8: VRFs + Route Targets")
    print("=" * 60)

    # Route targets first (source has 0 currently, but handle if present)
    src_rts = list(src.ipam.route_targets.all())
    if src_rts:
        print(f"  Source route targets: {len(src_rts)}")
        for rt in src_rts:
            try:
                existing = dst.ipam.route_targets.get(name=rt.name)
                if existing:
                    mapper.set("route_targets", rt.id, existing.id)
                    logger.info("Route target '%s' exists, skipped", rt.name)
                    continue
                if dry_run:
                    logger.info("[DRY RUN] Would create route target '%s'", rt.name)
                    continue
                new = dst.ipam.route_targets.create(
                    name=rt.name, description=rt.description or ""
                )
                mapper.set("route_targets", rt.id, new.id)
                logger.info("Created route target '%s'", rt.name)
            except Exception as e:
                logger.error("Failed route target '%s': %s", rt.name, e)

    src_vrfs = list(src.ipam.vrfs.all())
    print(f"  Source VRFs: {len(src_vrfs)}")

    for vrf in src_vrfs:
        try:
            existing = dst.ipam.vrfs.get(name=vrf.name)
            if existing:
                mapper.set("vrfs", vrf.id, existing.id)
                stats.skipped += 1
                logger.info("VRF '%s' exists, skipped", vrf.name)
                continue

            if dry_run:
                logger.info("[DRY RUN] Would create VRF '%s'", vrf.name)
                stats.created += 1
                continue

            data = {
                "name": vrf.name,
                "description": vrf.description or "",
                "enforce_unique": vrf.enforce_unique,
                "tags": _map_tags(vrf.tags, mapper),
            }
            if vrf.rd:
                data["rd"] = vrf.rd
            mapped_tenant = _mapped_fk(mapper, "tenants", vrf.tenant)
            if mapped_tenant:
                data["tenant"] = mapped_tenant

            new = dst.ipam.vrfs.create(data)
            mapper.set("vrfs", vrf.id, new.id)
            stats.created += 1
            logger.info("Created VRF '%s' (id=%d)", vrf.name, new.id)
        except Exception as e:
            stats.failed += 1
            logger.error("Failed VRF '%s': %s", vrf.name, e)

    stats.print_summary()
    return stats


# ---------------------------------------------------------------------------
# Phase 9: Prefixes  (sorted by prefix length — containers before children)
# ---------------------------------------------------------------------------

def migrate_prefixes(src, dst, mapper, dry_run):
    stats = PhaseStats("Prefixes")
    print("\n" + "=" * 60)
    print("Phase 9: Prefixes")
    print("=" * 60)

    src_prefixes = list(src.ipam.prefixes.all())
    src_prefixes.sort(
        key=lambda p: ipaddress.ip_network(p.prefix, strict=False).prefixlen
    )
    print(f"  Source: {len(src_prefixes)} (sorted by prefix length)")

    for pfx in src_prefixes:
        try:
            vrf_src_id = _obj_id(pfx.vrf)
            mapped_vrf = mapper.get("vrfs", vrf_src_id) if vrf_src_id else None

            # Dedup: match by (prefix, VRF) in destination
            existing = list(dst.ipam.prefixes.filter(prefix=pfx.prefix))
            already = False
            for ep in existing:
                ep_vrf = _obj_id(ep.vrf)
                if mapped_vrf is None and ep_vrf is None:
                    already = True
                    break
                if mapped_vrf is not None and ep_vrf == mapped_vrf:
                    already = True
                    break
            if already:
                stats.skipped += 1
                logger.info("Prefix '%s' exists, skipped", pfx.prefix)
                continue

            if dry_run:
                logger.info(
                    "[DRY RUN] Would create prefix '%s' (status=%s)",
                    pfx.prefix, _status_val(pfx.status),
                )
                stats.created += 1
                continue

            data = {
                "prefix": pfx.prefix,
                "status": _status_val(pfx.status),
                "description": pfx.description or "",
                "is_pool": pfx.is_pool,
                "mark_utilized": pfx.mark_utilized,
                "tags": _map_tags(pfx.tags, mapper),
            }
            if mapped_vrf:
                data["vrf"] = mapped_vrf

            mapped_vlan = _mapped_fk(mapper, "vlans", pfx.vlan)
            if mapped_vlan:
                data["vlan"] = mapped_vlan

            mapped_role = _mapped_fk(mapper, "roles", pfx.role)
            if mapped_role:
                data["role"] = mapped_role

            mapped_tenant = _mapped_fk(mapper, "tenants", pfx.tenant)
            if mapped_tenant:
                data["tenant"] = mapped_tenant

            new = dst.ipam.prefixes.create(data)
            stats.created += 1
            if stats.created % 25 == 0:
                logger.info("  ... %d / %d prefixes created", stats.created, len(src_prefixes))
            logger.info(
                "Created prefix '%s' (id=%d, status=%s)",
                pfx.prefix, new.id, data["status"],
            )
        except Exception as e:
            stats.failed += 1
            logger.error("Failed prefix '%s': %s", pfx.prefix, e)

    stats.print_summary()
    return stats


# ---------------------------------------------------------------------------
# Phase 10: IP Ranges
# ---------------------------------------------------------------------------

def migrate_ip_ranges(src, dst, mapper, dry_run):
    stats = PhaseStats("IP Ranges")
    print("\n" + "=" * 60)
    print("Phase 10: IP Ranges")
    print("=" * 60)

    src_ranges = list(src.ipam.ip_ranges.all())
    print(f"  Source: {len(src_ranges)}")

    # Pre-fetch all existing dest ranges for fast dedup
    dst_existing = set()
    for r in dst.ipam.ip_ranges.all():
        dst_existing.add((r.start_address, r.end_address))

    for ipr in src_ranges:
        try:
            if (ipr.start_address, ipr.end_address) in dst_existing:
                stats.skipped += 1
                logger.info(
                    "IP range %s–%s exists, skipped",
                    ipr.start_address, ipr.end_address,
                )
                continue

            if dry_run:
                logger.info(
                    "[DRY RUN] Would create IP range %s–%s",
                    ipr.start_address, ipr.end_address,
                )
                stats.created += 1
                continue

            data = {
                "start_address": ipr.start_address,
                "end_address": ipr.end_address,
                "status": _status_val(ipr.status),
                "description": ipr.description or "",
                "mark_utilized": ipr.mark_utilized,
                "tags": _map_tags(ipr.tags, mapper),
            }

            mapped_vrf = _mapped_fk(mapper, "vrfs", ipr.vrf)
            if mapped_vrf:
                data["vrf"] = mapped_vrf

            mapped_tenant = _mapped_fk(mapper, "tenants", ipr.tenant)
            if mapped_tenant:
                data["tenant"] = mapped_tenant

            mapped_role = _mapped_fk(mapper, "roles", ipr.role)
            if mapped_role:
                data["role"] = mapped_role

            new = dst.ipam.ip_ranges.create(data)
            dst_existing.add((ipr.start_address, ipr.end_address))
            stats.created += 1
            logger.info(
                "Created IP range %s–%s (id=%d)",
                ipr.start_address, ipr.end_address, new.id,
            )
        except Exception as e:
            stats.failed += 1
            logger.error(
                "Failed IP range %s–%s: %s",
                ipr.start_address, ipr.end_address, e,
            )

    stats.print_summary()
    return stats


# ---------------------------------------------------------------------------
# Verify-only mode
# ---------------------------------------------------------------------------

def verify(src, dst):
    print("\n" + "=" * 60)
    print("VERIFICATION REPORT")
    print("=" * 60)

    checks = [
        ("Tags", src.extras.tags, dst.extras.tags),
        ("Tenant Groups", src.tenancy.tenant_groups, dst.tenancy.tenant_groups),
        ("Tenants", src.tenancy.tenants, dst.tenancy.tenants),
        ("RIRs", src.ipam.rirs, dst.ipam.rirs),
        ("Aggregates", src.ipam.aggregates, dst.ipam.aggregates),
        ("IPAM Roles", src.ipam.roles, dst.ipam.roles),
        ("VLANs", src.ipam.vlans, dst.ipam.vlans),
        ("VRFs", src.ipam.vrfs, dst.ipam.vrfs),
        ("Prefixes", src.ipam.prefixes, dst.ipam.prefixes),
        ("IP Ranges", src.ipam.ip_ranges, dst.ipam.ip_ranges),
    ]

    all_ok = True
    print(f"\n  {'Object':<20} {'Source':>8} {'Dest':>8} {'Status':>10}")
    print("  " + "-" * 50)

    for name, src_ep, dst_ep in checks:
        src_count = len(list(src_ep.all()))
        dst_count = len(list(dst_ep.all()))
        if dst_count >= src_count:
            status = "OK"
        else:
            status = "MISSING"
            all_ok = False
        print(f"  {name:<20} {src_count:>8} {dst_count:>8} {status:>10}")

    print("  " + "-" * 50)
    if all_ok:
        print("\n  All destination counts match or exceed source.")
    else:
        print("\n  WARNING: Some objects are missing in destination.")

    return 0 if all_ok else 1


# ---------------------------------------------------------------------------
# Pre-populate mapper (for --phase N single-phase runs)
# ---------------------------------------------------------------------------

def _prepopulate_mapper(src, dst, mapper, up_to_phase):
    """Match existing dest objects to source IDs for phases before up_to_phase."""
    logger.info("Pre-populating ID mapper for phases 1–%d ...", up_to_phase - 1)

    if up_to_phase > 1:
        for tag in src.extras.tags.all():
            existing = dst.extras.tags.get(slug=tag.slug)
            if existing:
                mapper.set("tags", tag.id, existing.id)

    if up_to_phase > 2:
        for tg in src.tenancy.tenant_groups.all():
            existing = dst.tenancy.tenant_groups.get(slug=tg.slug)
            if existing:
                mapper.set("tenant_groups", tg.id, existing.id)

    if up_to_phase > 3:
        for t in src.tenancy.tenants.all():
            existing = dst.tenancy.tenants.get(slug=t.slug)
            if existing:
                mapper.set("tenants", t.id, existing.id)

    if up_to_phase > 4:
        for r in src.ipam.rirs.all():
            existing = dst.ipam.rirs.get(slug=r.slug)
            if existing:
                mapper.set("rirs", r.id, existing.id)

    if up_to_phase > 6:
        for role in src.ipam.roles.all():
            existing = dst.ipam.roles.get(slug=role.slug)
            if existing:
                mapper.set("roles", role.id, existing.id)

    if up_to_phase > 7:
        dst_vlans = {}
        for v in dst.ipam.vlans.all():
            dst_vlans.setdefault(v.vid, []).append(v)
        for v in src.ipam.vlans.all():
            candidates = dst_vlans.get(v.vid, [])
            for c in candidates:
                if c.name == v.name:
                    mapper.set("vlans", v.id, c.id)
                    candidates.remove(c)
                    break

    if up_to_phase > 8:
        for vrf in src.ipam.vrfs.all():
            existing = dst.ipam.vrfs.get(name=vrf.name)
            if existing:
                mapper.set("vrfs", vrf.id, existing.id)


# ---------------------------------------------------------------------------
# Phase registry + main
# ---------------------------------------------------------------------------

PHASES = {
    1: ("Tags", migrate_tags),
    2: ("Tenant Groups", migrate_tenant_groups),
    3: ("Tenants", migrate_tenants),
    4: ("RIRs", migrate_rirs),
    5: ("Aggregates", migrate_aggregates),
    6: ("IPAM Roles", migrate_roles),
    7: ("VLANs", migrate_vlans),
    8: ("VRFs", migrate_vrfs),
    9: ("Prefixes", migrate_prefixes),
    10: ("IP Ranges", migrate_ip_ranges),
}


def main():
    parser = argparse.ArgumentParser(
        description="Migrate IPAM data from old NetBox to new NetBox"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview migration without writing to destination",
    )
    parser.add_argument(
        "--verify-only", action="store_true",
        help="Compare source vs destination counts and report gaps",
    )
    parser.add_argument(
        "--phase", type=int, choices=range(1, 11), metavar="N",
        help="Run only phase N (1–10)",
    )
    args = parser.parse_args()

    src, dst = connect()

    if args.verify_only:
        return verify(src, dst)

    mapper = IdMapper()
    mode = "DRY RUN" if args.dry_run else "LIVE"

    print(f"\n{'=' * 60}")
    print(f"IPAM Migration — {mode}")
    print(f"{'=' * 60}")
    print(f"  Source:      {SRC_URL}")
    print(f"  Destination: {DST_URL}")

    if args.phase:
        phases_to_run = {args.phase: PHASES[args.phase]}
        if args.phase > 1:
            _prepopulate_mapper(src, dst, mapper, args.phase)
    else:
        phases_to_run = PHASES

    all_stats = []
    for num, (name, fn) in sorted(phases_to_run.items()):
        stats = fn(src, dst, mapper, args.dry_run)
        all_stats.append(stats)

    # Final summary
    print(f"\n{'=' * 60}")
    print("MIGRATION SUMMARY")
    print(f"{'=' * 60}")
    for s in all_stats:
        s.print_summary()

    total_c = sum(s.created for s in all_stats)
    total_u = sum(s.updated for s in all_stats)
    total_s = sum(s.skipped for s in all_stats)
    total_f = sum(s.failed for s in all_stats)
    print(f"\n  TOTAL: {total_c} created, {total_u} updated,"
          f" {total_s} skipped, {total_f} failed")

    if total_f:
        print(f"\n  WARNING: {total_f} failures — review log output above")

    return 1 if total_f else 0


if __name__ == "__main__":
    raise SystemExit(main())

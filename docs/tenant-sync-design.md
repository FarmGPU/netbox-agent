# Tenant Sync Architecture

## Status: Design — not yet implemented

## Problem

Tenant assignment in NetBox is currently manual. As the fleet grows and more
tenants are added (RunPod today, potentially others), we need automated
reconciliation between tenant APIs and NetBox.

Key requirements:
- Automatically assign `tenant` on devices based on tenant API data
- Track tenant-specific operational state (listed/unlisted on marketplace)
- Maintain a correlation key for debugging (tenant's internal machine ID)
- Support multiple tenants with different APIs

## Architecture

```
Tenant API (RunPod, etc.)
        │
        ▼
  ┌─────────────┐
  │ tenant-sync  │   Standalone service or cron job
  │              │   Runs every 5-15 min per tenant
  │  1. Query tenant API for machine inventory
  │  2. Correlate to NetBox device by asset_tag
  │  3. Write: tenant, tenant_status, tenant_machine_id, tenant_last_sync
  │  4. Mark devices absent from tenant response as "inactive"
  └──────┬──────┘
         ▼
      NetBox API
```

### Why not BMC API?

BMC API manages BMC lifecycle (enrollment, credentials, firmware). Tenant data
sync is a different domain with different cadence, failure modes, and data
sources. Mixing them creates a monolith. tenant-sync should be a separate
service or cron script.

### Why not netbox-agent?

netbox-agent runs on each host and reports local hardware. Tenant assignment
is a fleet-level concern that correlates external API data — it doesn't
belong on individual hosts.

## NetBox Custom Fields

| Field | Type | Values | Description |
|-------|------|--------|-------------|
| `tenant_status` | select | listed, unlisted, reserved, inactive | Operational state in tenant marketplace |
| `tenant_machine_id` | text | (free-form) | Tenant's internal machine/GPU ID |
| `tenant_last_sync` | datetime | (auto) | When tenant data was last reconciled |

These are orthogonal to device `status` (active/offline/etc.). A device can
be `active` in NetBox but `unlisted` in RunPod (pulled for maintenance).

The built-in `tenant` field (NetBox native) stores *who* (RunPod, Internal).
Custom fields store *what* (listed/unlisted) and *when* (last sync).

## Correlation Strategy

Join key priority:
1. **Asset tag** — most reliable (programmed via BMC API, unique, controlled)
2. **Serial number** — hardware-derived fallback
3. **GPU MAC / BMC MAC** — last resort (requires normalization)

Since all enrolled devices have asset tags from BMC API, this is the natural
foreign key for tenant correlation.

## Sync Logic (per tenant adapter)

```python
def sync_tenant(tenant_name: str, machines: list[dict]) -> dict:
    """Reconcile tenant machine list against NetBox.

    Each machine dict: {asset_tag, status, machine_id}
    """
    seen_tags = set()

    for m in machines:
        device = nb.dcim.devices.get(asset_tag=m["asset_tag"])
        if not device:
            log.warning("Tenant %s references unknown asset %s", tenant_name, m["asset_tag"])
            continue

        seen_tags.add(m["asset_tag"])
        cf = dict(device.custom_fields or {})
        update = False

        # Assign tenant
        if device.tenant is None or device.tenant.slug != tenant_name:
            device.tenant = get_tenant_id(tenant_name)
            update = True

        # Update tenant-specific fields
        if cf.get("tenant_status") != m["status"]:
            cf["tenant_status"] = m["status"]
            update = True
        if cf.get("tenant_machine_id") != m["machine_id"]:
            cf["tenant_machine_id"] = m["machine_id"]
            update = True

        cf["tenant_last_sync"] = datetime.now(timezone.utc).isoformat()
        device.custom_fields = cf

        if update:
            device.save()

    # Mark devices assigned to this tenant but NOT in the response
    stale = nb.dcim.devices.filter(tenant=tenant_name)
    for device in stale:
        if device.asset_tag not in seen_tags:
            cf = dict(device.custom_fields or {})
            if cf.get("tenant_status") != "inactive":
                cf["tenant_status"] = "inactive"
                cf["tenant_last_sync"] = datetime.now(timezone.utc).isoformat()
                device.custom_fields = cf
                device.save()
```

## Adding a New Tenant

1. Write a new adapter that queries the tenant's API and returns
   `[{asset_tag, status, machine_id}]`
2. Create the tenant in NetBox (Tenancy → Tenants)
3. Register the adapter in the sync service config
4. The sync loop calls each adapter on its own schedule

## Implementation Location

Options (in order of preference):
1. **Standalone script** in `fgpu_ansible/scripts/tenant-sync/` run via systemd timer
2. **Module in the existing RunPod data collection stack** (already has RunPod API creds)
3. **New microservice** (only if complexity warrants it)

Start with option 1 or 2. Promote to option 3 only if multi-tenant
scheduling, retries, or observability requirements grow.

## Schema Script Changes Needed

Add to `02_create_custom_field_choice_sets.py`:
```python
{
    "name": "TenantStatusChoices",
    "extra_choices": [
        ["listed", "Listed"],
        ["unlisted", "Unlisted"],
        ["reserved", "Reserved"],
        ["inactive", "Inactive"],
    ],
}
```

Add to `03_create_custom_fields.py`:
```python
{"name": "tenant_status", "type": "select", "choice_set": tenant_status_cs, ...},
{"name": "tenant_machine_id", "type": "text", ...},
{"name": "tenant_last_sync", "type": "datetime", ...},
```

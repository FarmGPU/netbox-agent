"""
NVIDIA BlueField DPU host.

Activated when running on a BlueField BSP / DOCA ARM Linux. The DPU is a
child Device of its parent host's device_bay in NetBox — netbox-agent on
the DPU must NEVER create a NetBox device record, only enrich an existing
one that was created by bmc-api during chassis-BMC discovery (Phase 1).

Dispatch: cli.py picks this class when dmidecode Chassis.Manufacturer is
"NVIDIA", or as a fallback when /sys/class/net/oob_net0 exists (the BSP
convention for the BlueField OOB management interface).
"""

import logging
import os
import socket

from netbox_agent.config import config
from netbox_agent.config import netbox_instance as nb
from netbox_agent.server import ServerBase


# BlueField BSP names the OOB management interface "oob_net0" by convention.
# Its MAC is the Mellanox firmware Base MAC — the same MAC that bmc-api's
# chassis-side enumeration sees, so it is the canonical join key for
# tying back to the NetBox record created by Phase 1 discovery.
_OOB_IFACE = "oob_net0"

# Skeletal-record marker dropped by the enrollment writer; removed by this
# agent once the record has been enriched with both serial and BMC MAC.
_SKELETAL_TAG = "dpu-needs-bmc-scan"


class BlueFieldHost(ServerBase):
    """
    BlueField DPU running netbox-agent on its own ARM Linux.

    Behaviour differs from a regular server in three places:

      - `_get_bmc_mac` reads the OOB management interface MAC instead of
        querying IPMI (the DPU OS has no IPMI access to anything useful).

      - `_netbox_create_server` refuses to create a device record. DPU
        rows are created exclusively by bmc-api discovery; a missing
        record at agent run-time indicates an ordering bug, not a new
        device.

      - is_blade / chassis methods report leaf-device values — the DPU is
        a child of its parent's device_bay, not a chassis or a blade.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.manufacturer = "NVIDIA"
        # ServerBase.__init__ tries IPMI for bmc_mac_address; on the BF3
        # ARM that always fails. Overwrite with the oob_net0 MAC so the
        # custom field is populated correctly at first sync.
        oob_mac = self._get_bmc_mac()
        if oob_mac:
            self.custom_fields["bmc_mac_address"] = oob_mac

    # --- Leaf-device declarations -----------------------------------

    def is_blade(self):
        return False

    def get_blade_slot(self):
        return None

    def get_chassis(self):
        return self.get_product_name()

    def get_chassis_name(self):
        return None

    def get_chassis_service_tag(self):
        return self.get_service_tag()

    # --- BlueField-specific overrides --------------------------------

    def _get_bmc_mac(self):
        """
        Return the DPU's OOB management MAC (= Mellanox firmware Base MAC).

        This is the canonical join key for tying this agent's report back
        to the NetBox device record created by bmc-api during chassis-BMC
        discovery. Returns None if oob_net0 is not present (which would
        indicate a BSP misconfiguration — agent run will then fail to
        find an existing record and exit via _netbox_create_server).
        """
        path = f"/sys/class/net/{_OOB_IFACE}/address"
        try:
            with open(path) as f:
                mac = f.read().strip().upper()
            if mac and mac != "00:00:00:00:00:00":
                return mac
        except (FileNotFoundError, PermissionError, OSError) as e:
            logging.warning("BlueField OOB MAC unavailable: %s (%s)", path, e)
        return None

    def _netbox_create_server(self, datacenter, tenant, rack):
        """
        Refuse to create a DPU device record from the agent side.

        DPU rows must be created by bmc-api / chassis-BMC discovery first
        (Phase 1 of the DPU enrollment pipeline). If lookup by asset_tag /
        serial / cf_bmc_mac_address all miss, surfacing a clear error is
        the correct behaviour — silently creating an orphan record would
        defeat the architecture.
        """
        raise RuntimeError(
            "BlueField DPU agent: no existing NetBox device matches this DPU "
            "(serial={serial!r}, oob_mac={mac!r}, parent={parent!r}, "
            "bay={bay!r}). DPU device records must be created by bmc-api / "
            "chassis-BMC discovery (or the enrollment writer) first — this "
            "agent runs in enrich-only mode.".format(
                serial=self.get_service_tag(),
                mac=self._get_bmc_mac(),
                parent=getattr(getattr(config, "dpu", None), "parent_device", None),
                bay=getattr(getattr(config, "dpu", None), "device_bay", None),
            )
        )

    # --- Skeletal-record lookup ------------------------------------

    def get_netbox_server(self, expansion=False):
        """
        Extend the base lookup chain (asset_tag → serial → BMC MAC) with
        a parent device_bay fallback for first-run skeletal records.

        Skeletals created by the enrollment writer carry no identity
        fields — only role, device_type, and a parent device_bay row.
        Once the agent has run once and populated serial + BMC MAC, the
        base class's normal chain matches and this fallback never fires.
        """
        server = super().get_netbox_server(expansion=expansion)
        if server is not None or expansion:
            return server

        parent_name = getattr(getattr(config, "dpu", None), "parent_device", None)
        bay_name = getattr(getattr(config, "dpu", None), "device_bay", None)
        if not (parent_name and bay_name):
            return None

        bay = nb.dcim.device_bays.get(device=parent_name, name=bay_name)
        if not bay or not getattr(bay, "installed_device", None):
            logging.debug(
                "DPU parent+bay fallback: no installed device in %s/%s",
                parent_name, bay_name,
            )
            return None

        # bay.installed_device is a nested record — refetch the full Device
        # so callers get a writable object with all fields populated.
        installed_id = bay.installed_device.id
        device = nb.dcim.devices.get(installed_id)
        if device:
            logging.info(
                "Matched DPU by parent device_bay: %s/%s → %s (id=%s)",
                parent_name, bay_name, device.name, device.id,
            )
        return device

    # --- Hostname pinning -----------------------------------------

    def get_hostname(self):
        """
        Honor dpu.netbox_name from config so the agent doesn't rename
        the canonical Device record (e.g. anaheim04-100x-dpu0) to
        whatever the BSP-default OS hostname is (often localhost or
        localhost.localdomain on a fresh DOCA install).

        Falls back to socket.gethostname() if the config key is unset,
        matching ServerBase behaviour for any DPU deployed without the
        host-context vars populated.
        """
        override = getattr(getattr(config, "dpu", None), "netbox_name", None)
        if override:
            return override
        if config.hostname_cmd is None:
            return socket.gethostname()
        import subprocess
        return subprocess.getoutput(config.hostname_cmd)

    # --- Post-sync skeletal tag cleanup ----------------------------

    def netbox_create_or_update(self, config, deps=None, network_only=False, state=None):
        """
        Wrap the base update flow so the dpu-needs-bmc-scan tag is dropped
        once the record carries both a system serial and a BMC MAC. The
        tag is the enrollment writer's "this record is a stub" marker; an
        agent run that produces real identity fields is exactly the event
        that should clear it.

        Conditional removal: if either field is still empty (e.g. dmidecode
        produced a placeholder, or oob_net0 was unavailable), leave the tag
        in place so downstream tooling still sees the record as pending.
        """
        super().netbox_create_or_update(
            config, deps=deps, network_only=network_only, state=state,
        )
        try:
            self._drop_skeletal_tag_if_enriched()
        except Exception as e:
            logging.warning("Skeletal tag cleanup failed (non-fatal): %s", e)

    def _drop_skeletal_tag_if_enriched(self):
        server = self.get_netbox_server()
        if server is None:
            return

        cf = dict(server.custom_fields or {})
        has_serial = bool((server.serial or "").strip())
        has_bmc_mac = bool((cf.get("bmc_mac_address") or "").strip())
        if not (has_serial and has_bmc_mac):
            logging.debug(
                "Skeletal tag retained on '%s' — serial=%r, bmc_mac=%r",
                server.name, server.serial, cf.get("bmc_mac_address"),
            )
            return

        tag_names = [getattr(t, "name", None) for t in (server.tags or [])]
        if _SKELETAL_TAG not in tag_names:
            return

        new_tags = [t.id for t in server.tags if getattr(t, "name", None) != _SKELETAL_TAG]
        server.tags = new_tags
        server.save()
        logging.info(
            "Dropped '%s' tag from '%s' (serial=%s, bmc_mac=%s)",
            _SKELETAL_TAG, server.name, server.serial, cf.get("bmc_mac_address"),
        )

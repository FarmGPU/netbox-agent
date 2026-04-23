"""
StateManager — Tracks local hardware state between agent runs.

Enables diff-based updates: only sync categories whose hardware
actually changed since the last successful run.
"""

import json
import logging
import os
import tempfile

logger = logging.getLogger("netbox_agent.state")


class StateManager:
    """
    Persist and compare hardware/network state between agent runs.

    State file lives at ``{state_dir}/last_state.json``.
    First run (no state file) triggers a full sync.
    Corrupt files are treated as missing (full sync).
    """

    def __init__(self, state_dir="/var/lib/netbox-agent-test"):
        self.state_dir = state_dir
        self.state_file = os.path.join(state_dir, "last_state.json")

    def load(self):
        """Load previous state. Returns dict or None (first run / corrupt)."""
        if not os.path.isfile(self.state_file):
            logger.info("No state file at %s — first run, full sync", self.state_file)
            return None
        try:
            with open(self.state_file, "r") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                raise ValueError("State file root is not a dict")
            return data
        except (json.JSONDecodeError, ValueError, OSError) as e:
            logger.warning("Corrupt state file %s (%s) — full sync", self.state_file, e)
            return None

    def save(self, hostname, hardware, network=None, dependencies=None):
        """
        Atomically write current state.

        Args:
            hostname: Device hostname
            hardware: dict of {category: [list of item dicts]}
            network: optional dict of network state
            dependencies: optional dict from check_all()
        """
        state = {
            "hostname": hostname,
            "hardware": hardware,
        }
        if network is not None:
            state["network"] = network
        if dependencies is not None:
            state["dependencies"] = dependencies

        os.makedirs(self.state_dir, exist_ok=True)

        # Atomic write: write to temp file, then rename
        fd, tmp_path = tempfile.mkstemp(
            dir=self.state_dir, prefix=".state_", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(state, f, indent=2, sort_keys=True)
            os.replace(tmp_path, self.state_file)
            logger.debug("State saved to %s", self.state_file)
        except Exception:
            # Clean up temp file on failure
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def diff_hardware(self, category, current_items):
        """
        Compare current hardware items against last saved state.

        Args:
            category: e.g. "cpu", "gpu", "dimm", "ssd", "nic", "psu"
            current_items: list of item dicts from detection

        Returns:
            (changed: bool, summary: str)
        """
        prev = self.load()
        if prev is None:
            return True, "no previous state"

        prev_hw = prev.get("hardware", {})
        prev_items = prev_hw.get(category, [])

        # Build comparable keys
        current_keys = self._item_keys(current_items)
        prev_keys = self._item_keys(prev_items)

        if current_keys == prev_keys:
            return False, "unchanged"

        added = current_keys - prev_keys
        removed = prev_keys - current_keys
        parts = []
        if added:
            parts.append(f"+{len(added)}")
        if removed:
            parts.append(f"-{len(removed)}")
        return True, ", ".join(parts)

    def diff_network(self, interfaces, ips):
        """
        Compare current network state against last saved state.

        Returns:
            (changed: bool, summary: str)
        """
        prev = self.load()
        if prev is None:
            return True, "no previous state"

        prev_net = prev.get("network", {})
        prev_ifaces = set(prev_net.get("interfaces", []))
        prev_ips = set(prev_net.get("ips", []))

        current_ifaces = set(interfaces) if interfaces else set()
        current_ips = set(ips) if ips else set()

        if current_ifaces == prev_ifaces and current_ips == prev_ips:
            return False, "unchanged"

        return True, "network changed"

    @staticmethod
    def _item_keys(items):
        """
        Build a set of hashable keys for hardware items.

        Uses serial if available, otherwise product+vendor.
        """
        keys = set()
        for item in items:
            serial = item.get("serial")
            if serial:
                keys.add(("serial", serial))
            else:
                product = item.get("product", "")
                vendor = item.get("vendor", "")
                keys.add(("pv", product, vendor))
        return keys

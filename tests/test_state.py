"""
Tests for netbox_agent.state — StateManager diff-based update tracker.

No netbox_agent.config dependency, so no pre-mocking needed.
"""

import json
import os

from netbox_agent.state import StateManager


class TestStateManager:

    def test_save_and_load_state(self, tmp_path):
        """Round-trip: save → load returns same data."""
        sm = StateManager(str(tmp_path))
        hardware = {
            "cpu": [{"product": "Xeon Gold 6430", "vendor": "Intel", "serial": None}],
            "gpu": [{"product": "A100", "vendor": "NVIDIA", "serial": "GPU-001"}],
        }
        sm.save("test-host", hardware, dependencies={"dmidecode": True})
        loaded = sm.load()

        assert loaded is not None
        assert loaded["hostname"] == "test-host"
        assert loaded["hardware"]["cpu"][0]["product"] == "Xeon Gold 6430"
        assert loaded["hardware"]["gpu"][0]["serial"] == "GPU-001"
        assert loaded["dependencies"]["dmidecode"] is True

    def test_diff_detects_hardware_change(self, tmp_path):
        """Changing a serial → changed=True."""
        sm = StateManager(str(tmp_path))
        hardware = {
            "gpu": [
                {"product": "A100", "vendor": "NVIDIA", "serial": "GPU-001"},
                {"product": "A100", "vendor": "NVIDIA", "serial": "GPU-002"},
            ],
        }
        sm.save("test-host", hardware)

        # Change one serial
        new_items = [
            {"product": "A100", "vendor": "NVIDIA", "serial": "GPU-001"},
            {"product": "A100", "vendor": "NVIDIA", "serial": "GPU-999"},
        ]
        changed, summary = sm.diff_hardware("gpu", new_items)
        assert changed is True
        assert "+1" in summary
        assert "-1" in summary

    def test_diff_detects_no_change(self, tmp_path):
        """Same data → changed=False."""
        sm = StateManager(str(tmp_path))
        hardware = {
            "gpu": [
                {"product": "A100", "vendor": "NVIDIA", "serial": "GPU-001"},
            ],
        }
        sm.save("test-host", hardware)

        same_items = [
            {"product": "A100", "vendor": "NVIDIA", "serial": "GPU-001"},
        ]
        changed, summary = sm.diff_hardware("gpu", same_items)
        assert changed is False
        assert summary == "unchanged"

    def test_first_run_no_state_file(self, tmp_path):
        """No state file → load returns None."""
        sm = StateManager(str(tmp_path))
        assert sm.load() is None

    def test_state_file_corrupt_falls_back(self, tmp_path):
        """Corrupt JSON → load returns None (triggers full sync)."""
        sm = StateManager(str(tmp_path))
        # Write garbage to the state file
        state_file = os.path.join(str(tmp_path), "last_state.json")
        with open(state_file, "w") as f:
            f.write("{{{{not valid json!!!")

        result = sm.load()
        assert result is None

    def test_diff_hardware_no_previous_state(self, tmp_path):
        """diff_hardware with no state file → changed=True."""
        sm = StateManager(str(tmp_path))
        items = [{"product": "Xeon", "vendor": "Intel", "serial": None}]
        changed, summary = sm.diff_hardware("cpu", items)
        assert changed is True
        assert "no previous state" in summary

    def test_diff_network(self, tmp_path):
        """Network diff detects interface changes."""
        sm = StateManager(str(tmp_path))
        sm.save("test-host", {}, network={"interfaces": ["eth0", "eth1"], "ips": ["10.0.0.1"]})

        # Same → unchanged
        changed, summary = sm.diff_network(["eth0", "eth1"], ["10.0.0.1"])
        assert changed is False

        # Different → changed
        changed, summary = sm.diff_network(["eth0", "eth1", "eth2"], ["10.0.0.1"])
        assert changed is True

    def test_atomic_write_creates_dir(self, tmp_path):
        """save() creates state_dir if it doesn't exist."""
        state_dir = os.path.join(str(tmp_path), "subdir", "nested")
        sm = StateManager(state_dir)
        sm.save("host", {"cpu": []})
        assert os.path.isfile(os.path.join(state_dir, "last_state.json"))

    def test_item_keys_serial_and_positional(self):
        """_item_keys uses serial when available, product+vendor otherwise."""
        items = [
            {"product": "A100", "vendor": "NVIDIA", "serial": "GPU-001"},
            {"product": "Xeon", "vendor": "Intel", "serial": None},
        ]
        keys = StateManager._item_keys(items)
        assert ("serial", "GPU-001") in keys
        assert ("pv", "Xeon", "Intel") in keys
        assert len(keys) == 2

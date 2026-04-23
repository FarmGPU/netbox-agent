"""
Tests for netbox_agent.dependencies — pre-flight tool checker.

This module has zero imports of netbox_agent.config, so no pre-mocking needed.
"""

from unittest.mock import patch

from netbox_agent.dependencies import check_all, get_missing, missing_deps_string, TOOLS


class TestDependencies:

    def test_check_all_with_all_available(self):
        """All tools present → all True."""
        with patch("netbox_agent.dependencies.which", return_value="/usr/bin/fake"):
            result = check_all()
        assert all(result.values())
        assert set(result.keys()) == set(TOOLS.keys())

    def test_check_all_with_missing_tools(self):
        """Some tools missing → mixed True/False."""
        def mock_which(name):
            return "/usr/bin/fake" if name in ("dmidecode", "lshw") else None

        with patch("netbox_agent.dependencies.which", side_effect=mock_which):
            result = check_all()
        assert result["dmidecode"] is True
        assert result["lshw"] is True
        assert result["ipmitool"] is False
        assert result["nvidia-smi"] is False

    def test_get_missing_returns_correct_list(self):
        """get_missing() returns only missing tool names."""
        def mock_which(name):
            return "/usr/bin/fake" if name in ("dmidecode", "lshw", "lsblk") else None

        with patch("netbox_agent.dependencies.which", side_effect=mock_which):
            missing = get_missing()
        assert "dmidecode" not in missing
        assert "lshw" not in missing
        assert "lsblk" not in missing
        assert "ipmitool" in missing
        assert "nvidia-smi" in missing

    def test_missing_deps_string_format(self):
        """missing_deps_string() returns sorted comma-separated string."""
        avail = {
            "dmidecode": True,
            "lshw": True,
            "ipmitool": False,
            "nvidia-smi": False,
            "lsblk": True,
        }
        result = missing_deps_string(avail)
        assert result == "ipmitool, nvidia-smi"

    def test_missing_deps_string_empty_when_all_present(self):
        """No missing tools → empty string."""
        avail = {name: True for name in TOOLS}
        assert missing_deps_string(avail) == ""

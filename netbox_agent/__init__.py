"""netbox-agent — hardware inventory agent for NetBox."""

from pathlib import Path
import re

# Read version from pyproject.toml (always authoritative, works for
# editable installs and rsync deployments without stale metadata).
try:
    _pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
    _match = re.search(r'version\s*=\s*"([^"]+)"', _pyproject.read_text())
    __version__ = _match.group(1) if _match else "unknown"
except Exception:
    # Fallback to package metadata (wheel/pip install)
    try:
        from importlib.metadata import version as _get_version
        __version__ = _get_version(__name__)
    except Exception:
        __version__ = "unknown"

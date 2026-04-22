from importlib.metadata import version as _get_version, PackageNotFoundError

try:
    __version__ = _get_version(__name__)
except PackageNotFoundError:
    # Editable install or rsync deployment — read from pyproject.toml
    try:
        from pathlib import Path
        import re
        _pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
        _match = re.search(r'version\s*=\s*"([^"]+)"', _pyproject.read_text())
        __version__ = _match.group(1) if _match else "unknown"
    except Exception:
        __version__ = "unknown"

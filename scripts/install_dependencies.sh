#!/usr/bin/env bash
# -------------------------------------------------------------------
# Install system packages required by netbox-agent.
#
# Supports: Ubuntu/Debian, RHEL/CentOS/Rocky/Alma, SUSE
#
# Usage:
#   sudo bash install_dependencies.sh          # install all
#   sudo bash install_dependencies.sh --check  # check only, no install
#
# Packages are grouped into:
#   CORE     — required for basic operation (lshw, dmidecode, iproute2)
#   NETWORK  — interface and neighbor discovery (ethtool, lldpd)
#   STORAGE  — disk detection and NVMe enrichment (nvme-cli, util-linux)
#   IPMI     — BMC/out-of-band management (ipmitool)
#   GPU      — NVIDIA GPU serial detection (nvidia-smi — NOT installed here)
#   EXTRAS   — useful for fixture collection (pciutils)
# -------------------------------------------------------------------
set -euo pipefail

CHECK_ONLY=false
if [[ "${1:-}" == "--check" ]]; then
    CHECK_ONLY=true
fi

# ── Detect distro ────────────────────────────────────────────────
detect_distro() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        echo "${ID:-unknown}"
    elif command -v lsb_release &>/dev/null; then
        lsb_release -si | tr '[:upper:]' '[:lower:]'
    else
        echo "unknown"
    fi
}

DISTRO=$(detect_distro)
echo "Detected distro: ${DISTRO}"

# ── Package lists per distro family ──────────────────────────────
# Tool name → apt package / yum package
# Format: "binary:apt_pkg:yum_pkg"
PACKAGES=(
    # CORE — required
    "lshw:lshw:lshw"
    "dmidecode:dmidecode:dmidecode"
    "ip:iproute2:iproute"
    # NETWORK
    "ethtool:ethtool:ethtool"
    "lldpctl:lldpd:lldpd"
    # STORAGE
    "nvme:nvme-cli:nvme-cli"
    "lsblk:util-linux:util-linux"
    "lscpu:util-linux:util-linux"
    # IPMI
    "ipmitool:ipmitool:ipmitool"
    # EXTRAS (useful for fixture collection and debugging)
    "lspci:pciutils:pciutils"
)

# ── Check and install ────────────────────────────────────────────
missing_apt=()
missing_yum=()
installed=()
not_found=()

for entry in "${PACKAGES[@]}"; do
    IFS=':' read -r binary apt_pkg yum_pkg <<< "$entry"
    if command -v "$binary" &>/dev/null; then
        installed+=("$binary")
    else
        not_found+=("$binary")
        missing_apt+=("$apt_pkg")
        missing_yum+=("$yum_pkg")
    fi
done

# Deduplicate package lists
missing_apt=($(echo "${missing_apt[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' '))
missing_yum=($(echo "${missing_yum[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' '))

echo ""
echo "=== Dependency Status ==="
echo "Installed: ${installed[*]:-none}"
echo "Missing:   ${not_found[*]:-none}"
echo ""

# nvidia-smi is special — installed via NVIDIA driver, not package manager
if command -v nvidia-smi &>/dev/null; then
    echo "nvidia-smi: found (GPU serial detection available)"
else
    echo "nvidia-smi: not found (install NVIDIA drivers separately for GPU servers)"
fi
echo ""

if [ ${#not_found[@]} -eq 0 ]; then
    echo "All dependencies satisfied."
    exit 0
fi

if $CHECK_ONLY; then
    echo "Run without --check to install missing packages."
    exit 1
fi

# ── Install ──────────────────────────────────────────────────────
echo "Installing: ${missing_apt[*]}"
echo ""

case "$DISTRO" in
    ubuntu|debian|pop|linuxmint)
        export DEBIAN_FRONTEND=noninteractive
        apt-get update -qq
        apt-get install -y -qq "${missing_apt[@]}"
        ;;
    rhel|centos|rocky|almalinux|fedora|ol)
        if command -v dnf &>/dev/null; then
            dnf install -y "${missing_yum[@]}"
        else
            yum install -y "${missing_yum[@]}"
        fi
        ;;
    sles|opensuse*)
        zypper install -y "${missing_yum[@]}"
        ;;
    *)
        echo "ERROR: Unsupported distro '${DISTRO}'. Install manually:"
        echo "  apt: ${missing_apt[*]}"
        echo "  yum: ${missing_yum[*]}"
        exit 1
        ;;
esac

# ── Enable lldpd if just installed ───────────────────────────────
if command -v lldpctl &>/dev/null && systemctl is-enabled lldpd &>/dev/null 2>&1; then
    echo ""
    echo "lldpd is installed. Ensuring service is enabled and running..."
    systemctl enable lldpd
    systemctl start lldpd
    echo "lldpd started. Note: LLDP neighbor data takes ~30s to populate."
fi

echo ""
echo "=== Installation complete ==="

# ── Verify ───────────────────────────────────────────────────────
echo ""
echo "Verification:"
for entry in "${PACKAGES[@]}"; do
    IFS=':' read -r binary _ _ <<< "$entry"
    if command -v "$binary" &>/dev/null; then
        echo "  [OK] $binary"
    else
        echo "  [MISSING] $binary"
    fi
done

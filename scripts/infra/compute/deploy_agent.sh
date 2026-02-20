#!/usr/bin/env bash
# deploy_agent.sh — Deploy netbox-agent to compute/storage nodes via SSH
#
# Supports Ubuntu (apt) and RHEL bootc (dnf --transient) automatically.
# Reads host inventory from JSON for IP/user lookup.
#
# Usage:
#   ./deploy_agent.sh <hostname>                # deploy + debug (read-only)
#   ./deploy_agent.sh <hostname> --update-all   # deploy + full update
#   ./deploy_agent.sh <ip>                      # deploy by IP directly
#   ./deploy_agent.sh --list                    # show all deployable hosts
#   ./deploy_agent.sh --list --group gpu        # filter by group
#   ./deploy_agent.sh --list --tier 0           # filter by risk tier
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
SSH_KEYS=("/home/fgpu/.ssh/fgpu" "/home/fgpu/.ssh/og-fgpu")
SSH_BASE_OPTS="-o ConnectTimeout=10 -o StrictHostKeyChecking=no -o BatchMode=yes"
INVENTORY="/tmp/full_host_inventory.json"

# Source NetBox credentials for the config file
ENV_FILE="${SCRIPT_DIR}/../.env"
if [[ -f "$ENV_FILE" ]]; then
    set -a; source "$ENV_FILE"; set +a
fi

# NetBox URL as seen from the compute nodes (management host IP)
NETBOX_URL_REMOTE="https://10.100.248.18"

# ── Inventory lookup ───────────────────────────────────────────────────────────
# Returns: ip user hostname
lookup_host() {
    local target="$1"
    if [[ ! -f "$INVENTORY" ]]; then
        echo "ERROR: Inventory not found at $INVENTORY" >&2
        return 1
    fi

    # Try matching by hostname first, then by mgmt_ip
    python3 -c "
import json, sys
with open('$INVENTORY') as f:
    hosts = json.load(f)
target = '$target'
for h in hosts:
    if h['hostname'] == target or h['mgmt_ip'] == target:
        print(h['mgmt_ip'], h.get('mgmt_user', 'fgpu'), h['hostname'])
        sys.exit(0)
# Not found in inventory — if it looks like an IP, use it directly
import re
if re.match(r'^\d+\.\d+\.\d+\.\d+$', target):
    print(target, 'fgpu', target)
    sys.exit(0)
print('', '', '')
sys.exit(1)
" 2>/dev/null
}

# ── List deployable hosts ──────────────────────────────────────────────────────
list_hosts() {
    local filter_group="${1:-}"
    local filter_tier="${2:-}"

    if [[ ! -f "$INVENTORY" ]]; then
        echo "ERROR: Inventory not found at $INVENTORY"
        exit 1
    fi

    python3 -c "
import json

with open('$INVENTORY') as f:
    hosts = json.load(f)

filter_group = '$filter_group'
filter_tier = '$filter_tier'

# Tier assignment: 0=open/no-service, 1=prod/storage, 2=prod/cpu, 3=prod/gpu
def get_tier(h):
    if h.get('type') == 'open' and not h.get('service'):
        return 0
    if h.get('group') == 'storage':
        return 1
    if h.get('group') == 'cpu':
        return 2
    if h.get('group') == 'gpu':
        return 3
    return 4  # infra/unknown

# Filter out infra services (no BMC, root-only like prom, loki, etc.)
deployable = [h for h in hosts if h.get('bmc_ip') or h.get('asset_tag')]

if filter_group:
    deployable = [h for h in deployable if h.get('group') == filter_group]
if filter_tier:
    tier_num = int(filter_tier)
    deployable = [h for h in deployable if get_tier(h) == tier_num]

deployable.sort(key=lambda h: (get_tier(h), h.get('hostname', '')))

tier_names = {0: 'open/idle', 1: 'storage', 2: 'cpu/prod', 3: 'gpu/prod', 4: 'other'}
current_tier = -1
count = 0

for h in deployable:
    t = get_tier(h)
    if t != current_tier:
        current_tier = t
        print(f'\n  === Tier {t}: {tier_names[t]} ===' )
    tag = h.get('asset_tag', '') or '-'
    svc = h.get('service', '') or '-'
    print(f'  {tag:>5s}  {h[\"hostname\"]:<55s}  {h[\"mgmt_ip\"]:<18s}  {h.get(\"mgmt_user\",\"fgpu\"):<6s}  {svc}')
    count += 1

print(f'\n  Total: {count} deployable hosts')
"
}

# ── Prepare local artifacts ────────────────────────────────────────────────────
prepare_artifacts() {
    echo "Preparing deployment artifacts..."

    # 1. Repo tarball (exclude heavy/sensitive dirs)
    tar czf /tmp/netbox-agent.tar.gz \
        --exclude='.git' --exclude='__pycache__' --exclude='*.pyc' \
        --exclude='.env' --exclude='netbox-docker' --exclude='tests/fixtures' \
        -C "${REPO_ROOT}" .
    echo "  Tarball: /tmp/netbox-agent.tar.gz ($(du -h /tmp/netbox-agent.tar.gz | cut -f1))"

    # 2. Config file (same for all hosts — site-level config)
    cat > /tmp/netbox_agent.yaml <<YAML
netbox:
  url: '${NETBOX_URL_REMOTE}'
  token: '${NETBOX_TOKEN}'
  ssl_verify: false

network:
  ignore_interfaces: '(dummy.*|docker.*|veth.*|br-.*|cni.*|podman.*|virbr.*|lo)'
  ignore_ips: '(127\.0\.0\..*|fe80.*|::1.*)'
  lldp: false
  ipmi: true

device:
  server_role: "Server"
  default_owner: "FarmGPU"

datacenter_location:
  driver: "cmd:echo smf01"
  regex: "(?P<datacenter>[A-Za-z0-9]+)"

inventory: true
YAML
    echo "  Config: /tmp/netbox_agent.yaml"

    # 3. System dependency installer (already multi-distro)
    cp "${REPO_ROOT}/scripts/install_dependencies.sh" /tmp/nb_install_deps.sh
    echo "  Deps: /tmp/nb_install_deps.sh"

    # 4. Bootstrap script (auto-detects OS, runs as root on remote)
    cat > /tmp/nb_bootstrap.sh <<'BOOTSTRAP'
#!/usr/bin/env bash
set -e

# ── Detect distro ──
if [ -f /etc/os-release ]; then
    . /etc/os-release
    DISTRO="${ID:-unknown}"
else
    DISTRO="unknown"
fi
echo "[0] Distro: ${DISTRO} (${PRETTY_NAME:-unknown})"

# ── Check if bootc (immutable OS) ──
IS_BOOTC=false
if command -v bootc &>/dev/null || [[ -f /run/.bootc ]]; then
    IS_BOOTC=true
    echo "    Bootc (immutable) detected — using transient packages"
fi

# ── System packages ──
echo "[1] System packages..."
if [ -f /tmp/nb_install_deps.sh ]; then
    if $IS_BOOTC; then
        # bootc: use transient overlay (resets on reboot)
        MISSING=()
        for cmd in lshw dmidecode ipmitool ethtool nvme lsblk lspci; do
            command -v "$cmd" >/dev/null 2>&1 || MISSING+=("$cmd")
        done

        if [ ${#MISSING[@]} -gt 0 ]; then
            echo "  Missing: ${MISSING[*]}"
            # Map command names to yum packages
            PKGS=()
            for cmd in "${MISSING[@]}"; do
                case "$cmd" in
                    lshw)      PKGS+=(lshw) ;;
                    dmidecode) PKGS+=(dmidecode) ;;
                    ipmitool)  PKGS+=(ipmitool) ;;
                    ethtool)   PKGS+=(ethtool) ;;
                    nvme)      PKGS+=(nvme-cli) ;;
                    lsblk)     PKGS+=(util-linux) ;;
                    lspci)     PKGS+=(pciutils) ;;
                esac
            done
            # Deduplicate
            PKGS=($(echo "${PKGS[@]}" | tr ' ' '\n' | sort -u))

            # Add CentOS Stream repos if RHEL subscription unavailable
            if [[ "$DISTRO" == "rhel" ]] && ! dnf --disablerepo="rhel-*" list lshw 2>/dev/null | grep -q lshw; then
                echo "  Adding CentOS Stream 10 repos..."
                cat > /etc/yum.repos.d/centos-stream-10.repo <<'CENTOS'
[centos-stream-10-baseos]
name=CentOS Stream 10 - BaseOS
baseurl=https://mirror.stream.centos.org/10-stream/BaseOS/x86_64/os/
gpgcheck=0
enabled=1

[centos-stream-10-appstream]
name=CentOS Stream 10 - AppStream
baseurl=https://mirror.stream.centos.org/10-stream/AppStream/x86_64/os/
gpgcheck=0
enabled=1
CENTOS
            fi

            echo "  Installing (transient): ${PKGS[*]}"
            if [[ "$DISTRO" == "rhel" ]]; then
                dnf --disablerepo="rhel-*" install -y --transient "${PKGS[@]}" 2>&1 | tail -5
            else
                dnf install -y --transient "${PKGS[@]}" 2>&1 | tail -5
            fi
        else
            echo "  All system packages present"
        fi
    else
        # Standard OS: use install_dependencies.sh
        bash /tmp/nb_install_deps.sh 2>&1 | tail -15
    fi
else
    echo "  WARN: install_dependencies.sh not found, skipping system packages"
fi

# ── pip ──
echo "[2] pip..."
if ! python3 -m pip --version >/dev/null 2>&1; then
    echo "  Installing pip..."
    if [[ "$DISTRO" == "ubuntu" || "$DISTRO" == "debian" ]]; then
        export DEBIAN_FRONTEND=noninteractive
        apt-get update -qq 2>&1 | tail -1
        apt-get install -y -qq python3-pip 2>&1 | tail -3
    else
        python3 -m ensurepip 2>&1 | tail -3
    fi
fi
# Verify pip works
if ! python3 -m pip --version >/dev/null 2>&1; then
    echo "  ERROR: pip install failed! Trying pipx/get-pip.py fallback..."
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3 --break-system-packages 2>&1 | tail -3
fi
echo "  $(python3 -m pip --version 2>&1 | head -1)"

# ── netbox-agent ──
echo "[3] netbox-agent..."
rm -rf /tmp/netbox-agent && mkdir -p /tmp/netbox-agent
tar xzf /tmp/netbox-agent.tar.gz -C /tmp/netbox-agent

# Install with --break-system-packages on newer Ubuntu (PEP 668)
# and --ignore-installed to avoid conflicts with system-managed packages (e.g. PyYAML)
PIP_ARGS="--quiet"
if python3 -c "import sys; sys.exit(0 if sys.version_info >= (3,11) else 1)" 2>/dev/null; then
    if [[ "$DISTRO" == "ubuntu" || "$DISTRO" == "debian" ]]; then
        PIP_ARGS="--quiet --break-system-packages --ignore-installed"
    fi
fi
python3 -m pip install $PIP_ARGS /tmp/netbox-agent/ 2>&1 | tail -10

# Verify install
AGENT_PATH=$(command -v netbox_agent 2>/dev/null || echo "")
if [[ -z "$AGENT_PATH" ]]; then
    # Check common locations
    for p in /usr/local/bin/netbox_agent /root/.local/bin/netbox_agent; do
        if [[ -x "$p" ]]; then AGENT_PATH="$p"; break; fi
    done
fi
if [[ -n "$AGENT_PATH" ]]; then
    echo "  Installed: $AGENT_PATH"
else
    echo "  ERROR: netbox_agent not found after install!"
    exit 1
fi

# ── Config ──
echo "[4] Config..."
cp /tmp/netbox_agent.yaml /etc/netbox_agent.yaml
echo "  Deployed /etc/netbox_agent.yaml"

echo "[OK] Bootstrap complete on $(hostname)"
BOOTSTRAP
    echo "  Bootstrap: /tmp/nb_bootstrap.sh"
}

# ── Deploy to one host ─────────────────────────────────────────────────────────
deploy_host() {
    local ip="$1"
    local user="$2"
    local hostname="$3"
    shift 3
    local agent_args=("$@")

    echo ""
    echo "================================================================"
    echo "  ${hostname} (${ip}) as ${user}"
    echo "================================================================"

    # Find working SSH key
    echo "  Checking connectivity..."
    local SSH_OPTS=""
    local key_found=false
    for key in "${SSH_KEYS[@]}"; do
        if [[ -f "$key" ]] && ssh -i "$key" ${SSH_BASE_OPTS} -o ConnectTimeout=5 ${user}@${ip} "echo ok" >/dev/null 2>&1; then
            SSH_OPTS="-i ${key} ${SSH_BASE_OPTS}"
            echo "  SSH key: $(basename $key)"
            key_found=true
            break
        fi
    done
    if ! $key_found; then
        echo "  ERROR: Cannot SSH to ${user}@${ip} with any key — skipping"
        return 1
    fi

    # Transfer artifacts
    echo "  Transferring artifacts..."
    scp -q ${SSH_OPTS} \
        /tmp/netbox-agent.tar.gz \
        /tmp/netbox_agent.yaml \
        /tmp/nb_bootstrap.sh \
        /tmp/nb_install_deps.sh \
        "${user}@${ip}:/tmp/"

    # Run bootstrap as root
    echo "  Bootstrapping..."
    ssh ${SSH_OPTS} ${user}@${ip} "sudo bash /tmp/nb_bootstrap.sh"

    # Determine PATH for agent (covers both /usr/local/bin and /root/.local/bin)
    local AGENT_ENV="sudo env PATH='/root/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin'"

    # Run the agent
    echo ""
    if [[ ${#agent_args[@]} -eq 0 ]]; then
        echo "  Running: netbox_agent --debug"
        ssh ${SSH_OPTS} ${user}@${ip} \
            "${AGENT_ENV} netbox_agent --debug 2>&1 | grep -v ^DEBUG" || true
    else
        echo "  Running: netbox_agent ${agent_args[*]}"
        ssh ${SSH_OPTS} ${user}@${ip} \
            "${AGENT_ENV} netbox_agent ${agent_args[*]} 2>&1 | grep -v ^DEBUG" || true
    fi

    echo ""
    echo "  Done: ${hostname} (${ip})"
}

# ── Main ───────────────────────────────────────────────────────────────────────
usage() {
    cat <<EOF
Usage: $0 <target> [agent-args...]
       $0 --list [--group GROUP] [--tier N]

Targets:
  <hostname>        Deploy to host by inventory hostname
  <ip>              Deploy to host by IP address

Agent args (passed to netbox_agent on remote):
  --debug           Read-only mode (default if no args given)
  --update-all      Full update: register + update all fields
  --register        Register device in NetBox
  --update-network  Update network interfaces only
  --update-inventory Update inventory items only

Options:
  --list            Show all deployable hosts from inventory
  --group GROUP     Filter --list by group (gpu, cpu, storage)
  --tier N          Filter --list by risk tier (0=open, 1=storage, 2=cpu, 3=gpu)

Examples:
  $0 smf01-2gnr-0001-d01-r03-c09-u32-storage-l-ginger01  --debug
  $0 10.100.200.20 --debug
  $0 lenovo-h200-1 --update-all
  $0 --list --tier 0
EOF
    exit 1
}

if [[ $# -lt 1 ]]; then
    usage
fi

# Handle --list mode
if [[ "$1" == "--list" ]]; then
    shift
    filter_group=""
    filter_tier=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --group) filter_group="$2"; shift 2 ;;
            --tier)  filter_tier="$2"; shift 2 ;;
            *) echo "Unknown option: $1"; exit 1 ;;
        esac
    done
    list_hosts "$filter_group" "$filter_tier"
    exit 0
fi

target="$1"
shift
agent_args=("$@")

# Lookup host from inventory
read -r ip user hostname <<< "$(lookup_host "$target")"

if [[ -z "$ip" ]]; then
    echo "ERROR: Host '$target' not found in inventory ($INVENTORY)"
    echo "Use --list to see available hosts."
    exit 1
fi

prepare_artifacts
deploy_host "$ip" "$user" "$hostname" "${agent_args[@]}"

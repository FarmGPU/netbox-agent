#!/usr/bin/env bash
# deploy_agent.sh — Deploy netbox-agent to RHEL bootc compute nodes via SSH
#
# These are bootc (immutable OS) nodes. System packages use --transient
# (overlay that resets on reboot). pip packages install into /root/.local/.
#
# Usage:
#   ./deploy_agent.sh potato01               # deploy + debug (no writes)
#   ./deploy_agent.sh potato01 --update-all  # deploy + full update
#   ./deploy_agent.sh all                    # deploy to all potato nodes
#   ./deploy_agent.sh all --update-all       # deploy + update all nodes
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
SSH_KEY="/home/fgpu/.ssh/fgpu"
SSH_USER="fgpu"
SSH_OPTS="-i ${SSH_KEY} -o ConnectTimeout=10 -o StrictHostKeyChecking=no"

# Source NetBox credentials for the config file
ENV_FILE="${SCRIPT_DIR}/../.env"
if [[ -f "$ENV_FILE" ]]; then
    set -a; source "$ENV_FILE"; set +a
fi

# NetBox URL as seen from the compute nodes (management host IP)
NETBOX_URL_REMOTE="https://10.100.248.18"

# ── Node IP map ──────────────────────────────────────────────────────────────
declare -A NODE_IPS=(
    [potato01]=10.100.200.26
    [potato02]=10.100.200.27
    [potato03]=10.100.200.28
    [potato04]=10.100.200.29
    [potato05]=10.100.200.30
    [potato06]=10.100.200.31
    [potato07]=10.100.200.32
)

ALL_NODES=(potato01 potato02 potato03 potato04 potato05 potato06 potato07)

# ── Prepare local artifacts ──────────────────────────────────────────────────
prepare_artifacts() {
    echo "Preparing deployment artifacts..."

    # 1. Repo tarball
    tar czf /tmp/netbox-agent.tar.gz \
        --exclude='.git' --exclude='__pycache__' --exclude='*.pyc' \
        --exclude='.env' --exclude='netbox-docker' \
        -C "${REPO_ROOT}" .
    echo "  Tarball: /tmp/netbox-agent.tar.gz ($(du -h /tmp/netbox-agent.tar.gz | cut -f1))"

    # 2. Config file
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

    # 3. Bootstrap script (runs on remote as root)
    cat > /tmp/nb_bootstrap.sh <<'BOOTSTRAP'
#!/usr/bin/env bash
set -e

echo "[1] System packages..."
MISSING=()
command -v lshw     >/dev/null 2>&1 || MISSING+=(lshw)
command -v ipmitool >/dev/null 2>&1 || MISSING+=(ipmitool)

if [ ${#MISSING[@]} -gt 0 ]; then
    # Add CentOS Stream 10 repos if needed (RHEL subscription may be unavailable)
    if ! dnf --disablerepo="rhel-*" list lshw 2>/dev/null | grep -q lshw; then
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
    echo "  Installing: ${MISSING[*]}"
    dnf --disablerepo="rhel-*" install -y --transient "${MISSING[@]}" 2>&1 | tail -5
else
    echo "  All system packages present"
fi

echo "[2] pip..."
if ! python3 -m pip --version >/dev/null 2>&1; then
    python3 -m ensurepip 2>&1 | tail -3
fi

echo "[3] netbox-agent..."
rm -rf /tmp/netbox-agent && mkdir -p /tmp/netbox-agent
tar xzf /tmp/netbox-agent.tar.gz -C /tmp/netbox-agent
python3 -m pip install --quiet /tmp/netbox-agent/ 2>&1 | tail -3

echo "[4] Config..."
cp /tmp/netbox_agent.yaml /etc/netbox_agent.yaml
echo "  Deployed /etc/netbox_agent.yaml"

echo "[OK] Bootstrap complete"
BOOTSTRAP
    echo "  Bootstrap: /tmp/nb_bootstrap.sh"
}

# ── Deploy to one node ───────────────────────────────────────────────────────
deploy_node() {
    local node="$1"
    shift
    local agent_args=("$@")
    local ip="${NODE_IPS[$node]}"

    if [[ -z "$ip" ]]; then
        echo "ERROR: Unknown node '$node'. Valid: ${!NODE_IPS[*]}"
        return 1
    fi

    echo ""
    echo "================================================================"
    echo "  ${node} (${ip})"
    echo "================================================================"

    # Transfer artifacts
    echo "  Transferring..."
    scp -q ${SSH_OPTS} \
        /tmp/netbox-agent.tar.gz \
        /tmp/netbox_agent.yaml \
        /tmp/nb_bootstrap.sh \
        "${SSH_USER}@${ip}:/tmp/"

    # Run bootstrap as root
    echo "  Bootstrapping..."
    ssh ${SSH_OPTS} ${SSH_USER}@${ip} "sudo bash /tmp/nb_bootstrap.sh"

    # Run the agent
    echo ""
    if [[ ${#agent_args[@]} -eq 0 ]]; then
        echo "  Running: netbox_agent --debug"
        ssh ${SSH_OPTS} ${SSH_USER}@${ip} \
            "sudo env PATH='/root/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' netbox_agent --debug 2>&1 | grep -v ^DEBUG" || true
    else
        echo "  Running: netbox_agent ${agent_args[*]}"
        ssh ${SSH_OPTS} ${SSH_USER}@${ip} \
            "sudo env PATH='/root/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' netbox_agent ${agent_args[*]} 2>&1 | grep -v ^DEBUG" || true
    fi

    echo "  Done: ${node}"
}

# ── Main ─────────────────────────────────────────────────────────────────────
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 {potato01..potato07|all} [--debug|--update-all|--register|...]"
    echo ""
    echo "Examples:"
    echo "  $0 potato01               # deploy + debug (read-only)"
    echo "  $0 potato01 --update-all  # deploy + full update"
    echo "  $0 all                    # deploy all (debug)"
    echo "  $0 all --update-all       # deploy + update all nodes"
    exit 1
fi

target="$1"
shift
agent_args=("$@")

prepare_artifacts

if [[ "$target" == "all" ]]; then
    successes=0
    failures=0
    for node in "${ALL_NODES[@]}"; do
        if deploy_node "$node" "${agent_args[@]}"; then
            successes=$((successes + 1))
        else
            echo "WARN: ${node} failed, continuing..."
            failures=$((failures + 1))
        fi
    done
    echo ""
    echo "================================================================"
    echo "  Summary: ${successes} succeeded, ${failures} failed"
    echo "================================================================"
else
    deploy_node "$target" "${agent_args[@]}"
fi

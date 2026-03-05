#!/usr/bin/env bash
# deploy_and_run.sh — Scan subnets, deploy netbox-agent-test, run on each host
#
# Two-phase operation:
#   Phase 1: Scan subnets with better-brute.py → discover SSH-accessible hosts
#   Phase 2: For each host, deploy netbox-agent to /opt/netbox-agent-test, run it
#
# Usage:
#   ./deploy_and_run.sh scan 192.168.211.0/24 10.100.191.0/24 10.100.200.0/24
#   ./deploy_and_run.sh deploy /path/to/scan_results.csv
#   ./deploy_and_run.sh deploy /path/to/scan_results.csv --dry-run
#   ./deploy_and_run.sh deploy /path/to/scan_results.csv --host 192.168.211.21
#   ./deploy_and_run.sh deploy /path/to/scan_results.csv --tunnel   # force tunnel for all
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AGENT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BRUTE_SCRIPT="/home/fgpu/brutes/os/better-brute.py"

# NetBox config — internal URL reachable from management network (10.100.x)
# NOTE: 192.168.211.x hosts CANNOT reach this — they need a tunnel
NETBOX_URL="https://10.100.248.18"
NETBOX_URL_TUNNEL="https://localhost:18443"
NETBOX_TOKEN="nbt_iKGuMp3OpEse.8O1M1A2PAgUJduwIhnpDZnAmgDxpRT9DVvftSj6o"
TUNNEL_PORT=18443

# Subnets that need an SSH tunnel to reach NetBox
# (management server = NetBox host, tunnel forwards remote:18443 → localhost:443)
TUNNEL_SUBNETS=("192.168.211." "192.168.213." "192.168.1." "10.100.191.")

# Jump host config — anaheim14 is dual-homed (10.100.191.46 + 192.168.211.175)
# Used for subnets not directly reachable from management server
JUMP_HOST="fgpu@10.100.191.46"
JUMP_KEY="$HOME/.ssh/fgpu"
JUMP_SUBNETS=("192.168.211." "192.168.213.")

# Remote paths
REMOTE_AGENT_DIR="/opt/netbox-agent-test"
REMOTE_CONFIG_DIR="/etc/netbox-agent-test"
REMOTE_CONFIG="${REMOTE_CONFIG_DIR}/config.yml"

# SSH settings
SSH_KEYS=("$HOME/.ssh/fgpu" "$HOME/.ssh/og-fgpu")
SSH_USERS=("fgpu" "root")
SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log()  { echo -e "${BLUE}[INFO]${NC} $*"; }
ok()   { echo -e "${GREEN}[OK]${NC}   $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; }

# ---------------------------------------------------------------------------
# Phase 1: Scan
# ---------------------------------------------------------------------------
cmd_scan() {
    local subnets=("$@")
    if [[ ${#subnets[@]} -eq 0 ]]; then
        echo "Usage: $0 scan <subnet1> [subnet2] ..."
        echo "Example: $0 scan 192.168.211.0/24 10.100.191.0/24"
        exit 1
    fi

    local timestamp
    timestamp=$(date +%Y%m%d_%H%M%S)
    local merged="/tmp/netbox-agent-scan_${timestamp}.csv"

    log "Scanning ${#subnets[@]} subnet(s) for SSH-accessible hosts..."

    local first=true
    for subnet in "${subnets[@]}"; do
        local out="/tmp/scan_${subnet//\//_}_${timestamp}.csv"
        log "Scanning $subnet ..."
        python3 "$BRUTE_SCRIPT" "$subnet" \
            -u "fgpu,root" \
            -k "$HOME/.ssh/fgpu,$HOME/.ssh/og-fgpu" \
            -o "$out" 2>/dev/null || true

        # Merge: keep header from first file only
        if $first; then
            cat "$out" >> "$merged"
            first=false
        else
            tail -n +2 "$out" >> "$merged" 2>/dev/null || true
        fi
    done

    local total
    total=$(( $(wc -l < "$merged") - 1 ))
    ok "Scan complete. $total hosts found."
    echo "Results: $merged"
    echo ""
    echo "Next: $0 deploy $merged"
    echo "       $0 deploy $merged --dry-run    # preview only"
}

# ---------------------------------------------------------------------------
# Tunnel detection
# ---------------------------------------------------------------------------
needs_tunnel() {
    local ip="$1"
    for prefix in "${TUNNEL_SUBNETS[@]}"; do
        if [[ "$ip" == ${prefix}* ]]; then
            return 0
        fi
    done
    return 1
}

needs_jump() {
    local ip="$1"
    for prefix in "${JUMP_SUBNETS[@]}"; do
        if [[ "$ip" == ${prefix}* ]]; then
            return 0
        fi
    done
    return 1
}

# ProxyCommand string for jump hosts (empty if not needed)
proxy_cmd_for() {
    local ip="$1"
    if needs_jump "$ip"; then
        echo "ssh -i ${JUMP_KEY} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -W %h:%p ${JUMP_HOST}"
    fi
}

# ---------------------------------------------------------------------------
# Generate netbox-agent config
# ---------------------------------------------------------------------------
generate_config() {
    local url="${1:-$NETBOX_URL}"
    cat <<YAML
netbox:
  url: '${url}'
  token: '${NETBOX_TOKEN}'
  ssl_verify: false

network:
  ignore_interfaces: '(dummy.*|docker.*|veth.*|br-.*|cni.*|podman.*|virbr.*|lo)'
  ignore_ips: '(127\\.0\\.0\\..*|fe80.*|::1.*)'
  lldp: false
  ipmi: true

device:
  server_role: 'Server'
  default_owner: 'FarmGPU'

datacenter_location:
  driver: 'cmd:echo smf01'
  regex: '(?P<datacenter>[A-Za-z0-9]+)'

inventory: true
YAML
}

# ---------------------------------------------------------------------------
# Phase 2: Deploy + Run
# ---------------------------------------------------------------------------
deploy_one_host() {
    local ip="$1"
    local user="$2"
    local hostname="$3"
    local dry_run="${4:-false}"
    local force_tunnel="${5:-false}"
    local label="${hostname:-$ip}"

    # Determine if this host needs a tunnel
    local use_tunnel=false
    if [[ "$force_tunnel" == "true" ]] || needs_tunnel "$ip"; then
        use_tunnel=true
    fi

    log "[$label] Deploying to $user@$ip ..."

    # Build proxy command for jump-host subnets
    local proxy_cmd
    proxy_cmd=$(proxy_cmd_for "$ip")
    local EXTRA_SSH_OPTS=("${SSH_OPTS[@]}")
    if [[ -n "$proxy_cmd" ]]; then
        EXTRA_SSH_OPTS+=(-o "ProxyCommand=$proxy_cmd")
        log "[$label] Using jump host ($JUMP_HOST)"
    fi

    # Find working SSH key  (-n prevents SSH from eating the while-read stdin)
    local ssh_key=""
    for key in "${SSH_KEYS[@]}"; do
        if ssh -n "${EXTRA_SSH_OPTS[@]}" -i "$key" "$user@$ip" "echo ok" &>/dev/null; then
            ssh_key="$key"
            break
        fi
    done

    if [[ -z "$ssh_key" ]]; then
        fail "[$label] No working SSH key found"
        return 1
    fi

    local SSH=(ssh -n "${EXTRA_SSH_OPTS[@]}" -i "$ssh_key" "$user@$ip")
    local SCP=(scp "${EXTRA_SSH_OPTS[@]}" -i "$ssh_key")

    # VM detection — skip virtual machines (Proxmox KVM, QEMU, etc.)
    local virt_type
    virt_type=$("${SSH[@]}" "systemd-detect-virt 2>/dev/null || echo unknown" 2>/dev/null)
    virt_type=$(echo "$virt_type" | head -1 | tr -d '[:space:]')
    if [[ "$virt_type" != "none" && -n "$virt_type" ]]; then
        warn "[$label] Skipping — virtual machine detected (type: $virt_type)"
        return 2  # distinct return code for VM skip
    fi

    # Check if we can sudo
    if ! "${SSH[@]}" "sudo -n true" &>/dev/null; then
        fail "[$label] Cannot sudo without password"
        return 1
    fi

    if [[ "$dry_run" == "true" ]]; then
        local tunnel_note=""
        if [[ "$use_tunnel" == "true" ]]; then
            tunnel_note=" [tunnel]"
        fi
        ok "[$label] Would deploy (user=$user, key=$(basename "$ssh_key"))${tunnel_note}"
        return 0
    fi

    # Step 1: Create remote directories (detect read-only /opt)
    local remote_agent_dir="$REMOTE_AGENT_DIR"
    local remote_config_dir="$REMOTE_CONFIG_DIR"
    local remote_config="${remote_config_dir}/config.yml"
    log "[$label] Creating remote directories..."
    if ! "${SSH[@]}" "sudo mkdir -p $REMOTE_AGENT_DIR $REMOTE_CONFIG_DIR" 2>/dev/null; then
        # /opt is read-only (composefs, etc.) — use /var/lib instead
        remote_agent_dir="/var/lib/netbox-agent-test"
        remote_config_dir="/var/lib/netbox-agent-test-config"
        remote_config="${remote_config_dir}/config.yml"
        warn "[$label] /opt is read-only, using $remote_agent_dir"
        "${SSH[@]}" "sudo mkdir -p $remote_agent_dir $remote_config_dir" || {
            fail "[$label] Failed to create directories"; return 1
        }
    fi

    # Step 2: Create tarball of agent code (exclude .git, __pycache__)
    local tarball="/tmp/netbox-agent-test.tar.gz"
    tar czf "$tarball" \
        -C "$(dirname "$AGENT_ROOT")" \
        --exclude='.git' \
        --exclude='__pycache__' \
        --exclude='*.pyc' \
        --exclude='.eggs' \
        --exclude='*.egg-info' \
        "$(basename "$AGENT_ROOT")" 2>/dev/null

    # Step 3: Upload tarball
    log "[$label] Uploading agent code..."
    "${SCP[@]}" "$tarball" "$user@$ip:/tmp/netbox-agent-test.tar.gz" &>/dev/null || {
        fail "[$label] SCP failed"; return 1
    }

    # Step 4: Extract and install
    # Pass remote_agent_dir as environment variable for the heredoc
    log "[$label] Installing agent..."
    ssh "${EXTRA_SSH_OPTS[@]}" -i "$ssh_key" "$user@$ip" sh -s "$remote_agent_dir" <<'REMOTE_INSTALL'
set -e
AGENT_DIR="$1"

# Extract tarball into temp dir, then move to target
sudo rm -rf "$AGENT_DIR"
sudo mkdir -p /tmp/netbox-agent-extract
sudo tar xzf /tmp/netbox-agent-test.tar.gz -C /tmp/netbox-agent-extract
# Move extracted contents to target dir
sudo mkdir -p "$AGENT_DIR"
if [ -d /tmp/netbox-agent-extract/netbox-agent ]; then
    sudo cp -a /tmp/netbox-agent-extract/netbox-agent/* "$AGENT_DIR/"
fi
sudo rm -rf /tmp/netbox-agent-extract

# Find pip
PIP=""
for p in pip3 pip; do
    if command -v "$p" >/dev/null 2>&1; then
        PIP="$p"
        break
    fi
done

if [ -z "$PIP" ]; then
    echo "WARN: No pip found, trying to install python3-pip"
    if command -v apt-get >/dev/null 2>&1; then
        sudo apt-get install -y -qq python3-pip 2>/dev/null || true
    elif command -v dnf >/dev/null 2>&1; then
        sudo dnf install -y -q python3-pip 2>/dev/null || true
    fi
    PIP=$(command -v pip3 2>/dev/null || command -v pip 2>/dev/null || echo "")
fi

if [ -n "$PIP" ]; then
    # Install deps — use --break-system-packages + --ignore-installed for PEP 668 distros
    cd "$AGENT_DIR"
    sudo "$PIP" install --break-system-packages --ignore-installed -r requirements.txt 2>&1 || \
    sudo "$PIP" install -r requirements.txt 2>&1 || \
    sudo "$PIP" install --break-system-packages -r requirements.txt 2>&1 || true
else
    echo "ERROR: pip not available"
fi

rm -f /tmp/netbox-agent-test.tar.gz
REMOTE_INSTALL

    if [[ $? -ne 0 ]]; then
        fail "[$label] Installation failed"
        return 1
    fi

    # Step 5: Upload config (pipe stdin, so don't use -n)
    # Use tunnel URL for hosts that can't reach NetBox directly
    log "[$label] Writing config..."
    if [[ "$use_tunnel" == "true" ]]; then
        log "[$label] Using tunnel URL ($NETBOX_URL_TUNNEL)"
        generate_config "$NETBOX_URL_TUNNEL" | ssh "${EXTRA_SSH_OPTS[@]}" -i "$ssh_key" "$user@$ip" "sudo tee $remote_config > /dev/null"
    else
        generate_config "$NETBOX_URL" | ssh "${EXTRA_SSH_OPTS[@]}" -i "$ssh_key" "$user@$ip" "sudo tee $remote_config > /dev/null"
    fi

    # Step 6: Check system dependencies
    ssh "${EXTRA_SSH_OPTS[@]}" -i "$ssh_key" "$user@$ip" sh -s <<'REMOTE_DEPS'
missing=""
for tool in dmidecode ipmitool lshw lsblk lscpu ethtool nvme-cli; do
    # nvme-cli installs the "nvme" binary — check for both
    check_name="$tool"
    if [ "$tool" = "nvme-cli" ]; then
        check_name="nvme"
    fi
    if ! command -v $check_name &>/dev/null; then
        missing="$missing $tool"
    fi
done
if [ -n "$missing" ]; then
    echo "WARN: Missing system tools:$missing (attempting install)"
    # Try to install missing tools
    if command -v apt-get &>/dev/null; then
        sudo apt-get install -y -qq $missing 2>/dev/null || true
    elif command -v dnf &>/dev/null; then
        sudo dnf install -y -q $missing 2>/dev/null || true
    elif command -v zypper &>/dev/null; then
        sudo zypper install -y -q $missing 2>/dev/null || true
    fi
fi
REMOTE_DEPS

    # Step 7: Run the agent (PYTHONPATH points to our source tree)
    # For tunnel hosts: SSH with -R to forward remote:18443 → localhost:443 (NetBox)
    log "[$label] Running netbox-agent --update-all ..."
    local output
    if [[ "$use_tunnel" == "true" ]]; then
        log "[$label] Opening SSH tunnel (remote:$TUNNEL_PORT → localhost:443)"
        output=$(ssh -n "${EXTRA_SSH_OPTS[@]}" -i "$ssh_key" \
            -R "${TUNNEL_PORT}:127.0.0.1:443" \
            "$user@$ip" \
            "sudo PYTHONPATH=$remote_agent_dir python3 -m netbox_agent.cli -c $remote_config --update-all 2>&1" \
            2>/dev/null) || true
    else
        output=$("${SSH[@]}" "sudo PYTHONPATH=$remote_agent_dir python3 -m netbox_agent.cli -c $remote_config --update-all 2>&1" 2>/dev/null) || true
    fi

    # Filter out known non-critical warnings before checking for real errors
    local filtered_output
    filtered_output=$(echo "$output" | grep -vi \
        -e "Cannot report power consumption" \
        -e "IPMI decoding failed" \
        -e "pip.*root.*user" \
        -e "WARNING:.*Running pip" \
        -e "error: externally-managed-environment")
    if echo "$filtered_output" | grep -qi "exception\|traceback"; then
        warn "[$label] Agent ran with errors:"
        echo "$output" | grep -i "error\|exception\|traceback" | head -5
        return 1
    else
        ok "[$label] Agent completed successfully"
        # Show key info
        echo "$output" | grep -i "creat\|updat\|match\|asset\|bmc" | head -5 || true
    fi

    return 0
}

cmd_deploy() {
    local csv_file="$1"; shift
    local dry_run=false
    local single_host=""
    local force_tunnel=false

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --dry-run) dry_run=true ;;
            --host) single_host="$2"; shift ;;
            --tunnel) force_tunnel=true ;;
            *) echo "Unknown option: $1"; exit 1 ;;
        esac
        shift
    done

    if [[ ! -f "$csv_file" ]]; then
        echo "Error: CSV file not found: $csv_file"
        exit 1
    fi

    log "Reading scan results from $csv_file ..."

    local total=0 success=0 failed=0 skipped=0

    # Read CSV on fd 3 to prevent SSH from consuming stdin
    while IFS=',' read -r -u 3 user ip hostname type group service asset_tag bmc_ip bmc_mac; do
        [[ "$user" == "user" ]] && continue  # skip header

        # Filter to single host if specified
        if [[ -n "$single_host" && "$ip" != "$single_host" ]]; then
            continue
        fi

        # Skip proxmox containers — they're not physical hosts
        if [[ "$type" == "proxmox" ]]; then
            warn "[$hostname] Skipping proxmox container"
            ((skipped++)) || true
            continue
        fi

        ((total++)) || true

        deploy_one_host "$ip" "$user" "$hostname" "$dry_run" "$force_tunnel"
        local rc=$?
        if [[ $rc -eq 0 ]]; then
            ((success++)) || true
        elif [[ $rc -eq 2 ]]; then
            ((skipped++)) || true  # VM detected
        else
            ((failed++)) || true
        fi
    done 3< "$csv_file"

    echo ""
    echo "=========================================="
    if [[ "$dry_run" == "true" ]]; then
        echo "  DRY RUN SUMMARY"
    else
        echo "  DEPLOYMENT SUMMARY"
    fi
    echo "=========================================="
    echo "  Total:   $total"
    echo "  Success: $success"
    echo "  Failed:  $failed"
    echo "  Skipped: $skipped"
    echo "=========================================="
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
case "${1:-help}" in
    scan)
        shift
        cmd_scan "$@"
        ;;
    deploy)
        shift
        cmd_deploy "$@"
        ;;
    config)
        generate_config
        ;;
    help|--help|-h)
        cat <<EOF
netbox-agent-test deployment pipeline

Commands:
  scan <subnet> [subnet...]     Scan subnets for SSH-accessible hosts
  deploy <csv> [options]        Deploy and run netbox-agent on discovered hosts
  config                        Print the generated config (for inspection)

Deploy options:
  --dry-run                     Preview which hosts would be deployed to
  --host <ip>                   Deploy to a single host only
  --tunnel                      Force SSH tunnel for all hosts (auto-detected for 192.168.x)

Examples:
  # Step 1: Scan all plausible subnets
  $0 scan 192.168.211.0/24 10.100.191.0/24 10.100.200.0/24

  # Step 2: Preview deployment
  $0 deploy /tmp/netbox-agent-scan_TIMESTAMP.csv --dry-run

  # Step 3: Deploy to all discovered hosts
  $0 deploy /tmp/netbox-agent-scan_TIMESTAMP.csv

  # Or deploy to just one host first
  $0 deploy /tmp/netbox-agent-scan_TIMESTAMP.csv --host 192.168.211.21
EOF
        ;;
    *)
        echo "Unknown command: $1"
        echo "Run '$0 help' for usage."
        exit 1
        ;;
esac

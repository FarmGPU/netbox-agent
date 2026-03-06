#!/usr/bin/env bash
# orchestrate.sh — Parallel netbox-agent deployment and execution
#
# Deploys netbox-agent to multiple servers in parallel via SSH, installs
# dependencies, writes config, and runs the agent. Tries SSH keys in order
# until one works for each host.
#
# Usage:
#   # Deploy to specific IPs
#   ./orchestrate.sh --ips 10.100.200.44,10.100.200.45 --keys ~/.ssh/fgpu,~/.ssh/og-fgpu
#
#   # Deploy from a file (one IP per line)
#   ./orchestrate.sh --ips-file hosts.txt --keys ~/.ssh/fgpu,~/.ssh/og-fgpu
#
#   # Preview without deploying
#   ./orchestrate.sh --ips 10.100.200.44 --keys ~/.ssh/fgpu --dry-run
#
#   # Custom user and parallelism
#   ./orchestrate.sh --ips-file hosts.txt --keys ~/.ssh/fgpu --user root --parallel 5
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AGENT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
SSH_USER="fgpu"
MAX_PARALLEL=10
DRY_RUN=false
IPS=()
SSH_KEYS=()

# NetBox
NETBOX_URL="https://10.100.248.18"
NETBOX_URL_TUNNEL="https://localhost:18443"
NETBOX_TOKEN="nbt_iKGuMp3OpEse.8O1M1A2PAgUJduwIhnpDZnAmgDxpRT9DVvftSj6o"
TUNNEL_PORT=18443

# Remote paths
REMOTE_AGENT_DIR="/opt/netbox-agent-test"
REMOTE_CONFIG_DIR="/etc/netbox-agent-test"

# Subnets requiring SSH tunnel to reach NetBox
TUNNEL_SUBNETS=("192.168.211." "192.168.213." "192.168.1." "10.100.191." "10.100.208." "10.100.10.")

# Jump host — for subnets not directly reachable from this machine
JUMP_HOST="fgpu@10.100.191.46"
JUMP_SUBNETS=("192.168.211." "192.168.213.")

# SSH options
SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=15 -o LogLevel=ERROR)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# ---------------------------------------------------------------------------
# Logging (per-host logs go to files; summary goes to stdout)
# ---------------------------------------------------------------------------
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_DIR="/tmp/netbox-orchestrate-${TIMESTAMP}"

log()  { echo -e "${BLUE}[INFO]${NC} $*"; }
ok()   { echo -e "${GREEN}[ OK ]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; }

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
usage() {
    cat <<EOF
Usage: $(basename "$0") --ips <ip,ip,...> --keys <key,key,...> [options]
       $(basename "$0") --ips-file <file> --keys <key,key,...> [options]

Required:
  --ips <ip,ip,...>       Comma-separated list of target IPs
  --ips-file <file>       File with one IP per line (alternative to --ips)
  --keys <key,key,...>    Comma-separated list of SSH private key paths (tried in order)

Options:
  --user <user>           SSH username (default: fgpu)
  --parallel <n>          Max concurrent deployments (default: 10)
  --dry-run               Test SSH connectivity only, don't deploy
  --help                  Show this help

Examples:
  # Deploy to 3 hosts, trying two keys
  $(basename "$0") --ips 10.100.200.44,10.100.200.45,10.100.200.46 \\
                   --keys ~/.ssh/fgpu,~/.ssh/og-fgpu

  # Deploy to hosts from ARP discovery, dry-run first
  $(basename "$0") --ips-file discovered_hosts.txt --keys ~/.ssh/fgpu --dry-run

  # Deploy to GPU subnet with custom parallelism
  $(basename "$0") --ips-file gpu_hosts.txt --keys ~/.ssh/fgpu --parallel 5 --user root
EOF
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ips)
            IFS=',' read -ra IPS <<< "$2"; shift 2 ;;
        --ips-file)
            if [[ ! -f "$2" ]]; then
                echo "Error: file not found: $2"; exit 1
            fi
            while IFS= read -r line; do
                line="${line%%#*}"         # strip comments
                line="${line// /}"         # strip spaces
                [[ -n "$line" ]] && IPS+=("$line")
            done < "$2"
            shift 2 ;;
        --keys)
            IFS=',' read -ra SSH_KEYS <<< "$2"; shift 2 ;;
        --user)
            SSH_USER="$2"; shift 2 ;;
        --parallel)
            MAX_PARALLEL="$2"; shift 2 ;;
        --dry-run)
            DRY_RUN=true; shift ;;
        --help|-h)
            usage 0 ;;
        *)
            echo "Unknown option: $1"; usage 1 ;;
    esac
done

# Validate
if [[ ${#IPS[@]} -eq 0 ]]; then
    echo "Error: no IPs specified. Use --ips or --ips-file."
    usage 1
fi
if [[ ${#SSH_KEYS[@]} -eq 0 ]]; then
    echo "Error: no SSH keys specified. Use --keys."
    usage 1
fi

# Expand ~ in key paths and validate
EXPANDED_KEYS=()
for key in "${SSH_KEYS[@]}"; do
    expanded="${key/#\~/$HOME}"
    if [[ ! -f "$expanded" ]]; then
        warn "SSH key not found: $expanded (skipping)"
    else
        EXPANDED_KEYS+=("$expanded")
    fi
done
if [[ ${#EXPANDED_KEYS[@]} -eq 0 ]]; then
    fail "No valid SSH keys found"; exit 1
fi
SSH_KEYS=("${EXPANDED_KEYS[@]}")

# ---------------------------------------------------------------------------
# Network helpers
# ---------------------------------------------------------------------------
needs_tunnel() {
    local ip="$1"
    for prefix in "${TUNNEL_SUBNETS[@]}"; do
        [[ "$ip" == ${prefix}* ]] && return 0
    done
    return 1
}

needs_jump() {
    local ip="$1"
    for prefix in "${JUMP_SUBNETS[@]}"; do
        [[ "$ip" == ${prefix}* ]] && return 0
    done
    return 1
}

proxy_cmd_for() {
    local ip="$1"
    if needs_jump "$ip"; then
        # Find a working key for the jump host
        local jkey=""
        for k in "${SSH_KEYS[@]}"; do
            if ssh -o BatchMode=yes -o StrictHostKeyChecking=no \
                   -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 \
                   -o LogLevel=ERROR -i "$k" "$JUMP_HOST" "echo ok" &>/dev/null; then
                jkey="$k"
                break
            fi
        done
        if [[ -n "$jkey" ]]; then
            echo "ssh -i $jkey -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -W %h:%p $JUMP_HOST"
        else
            echo "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -W %h:%p $JUMP_HOST"
        fi
    fi
}

# ---------------------------------------------------------------------------
# Config generator
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
# Build tarball (once, before parallel deployment)
# ---------------------------------------------------------------------------
TARBALL="/tmp/netbox-agent-test-${TIMESTAMP}.tar.gz"

build_tarball() {
    log "Building agent tarball..."
    tar czf "$TARBALL" \
        -C "$(dirname "$AGENT_ROOT")" \
        --exclude='.git' \
        --exclude='__pycache__' \
        --exclude='*.pyc' \
        --exclude='.eggs' \
        --exclude='*.egg-info' \
        --exclude='.venv' \
        --exclude='node_modules' \
        "$(basename "$AGENT_ROOT")" 2>/dev/null
    local size
    size=$(du -h "$TARBALL" | cut -f1)
    log "Tarball ready: $TARBALL ($size)"
}

# ---------------------------------------------------------------------------
# Per-host deployment (runs as background job)
# ---------------------------------------------------------------------------
deploy_host() {
    local ip="$1"
    local logfile="$LOG_DIR/${ip}.log"

    exec > "$logfile" 2>&1

    echo "=== Deploying to $ip at $(date) ==="
    echo ""

    # --- Determine connectivity ---
    local use_tunnel=false
    needs_tunnel "$ip" && use_tunnel=true

    local proxy_cmd
    proxy_cmd=$(proxy_cmd_for "$ip")
    local EXTRA_SSH_OPTS=("${SSH_OPTS[@]}")
    if [[ -n "$proxy_cmd" ]]; then
        EXTRA_SSH_OPTS+=(-o "ProxyCommand=$proxy_cmd")
        echo "[INFO] Using jump host ($JUMP_HOST)"
    fi

    # --- Try SSH keys in order ---
    local ssh_key=""
    for key in "${SSH_KEYS[@]}"; do
        if ssh -n "${EXTRA_SSH_OPTS[@]}" -i "$key" "$SSH_USER@$ip" "echo ok" &>/dev/null; then
            ssh_key="$key"
            break
        fi
    done

    if [[ -z "$ssh_key" ]]; then
        echo "[FAIL] No working SSH key for $SSH_USER@$ip"
        echo "STATUS:auth_failed"
        return 1
    fi
    echo "[INFO] Authenticated with key: $(basename "$ssh_key")"

    local SSH=(ssh -n "${EXTRA_SSH_OPTS[@]}" -i "$ssh_key" "$SSH_USER@$ip")
    local SCP=(scp "${EXTRA_SSH_OPTS[@]}" -i "$ssh_key")

    # --- Get hostname ---
    local hostname
    hostname=$("${SSH[@]}" "hostname -s 2>/dev/null || hostname" 2>/dev/null | head -1 | tr -d '[:space:]')
    echo "[INFO] Hostname: ${hostname:-unknown}"

    # --- VM detection ---
    local virt_type
    virt_type=$("${SSH[@]}" "systemd-detect-virt 2>/dev/null || echo none" 2>/dev/null | head -1 | tr -d '[:space:]')
    if [[ "$virt_type" != "none" && -n "$virt_type" ]]; then
        echo "[SKIP] Virtual machine detected (type: $virt_type)"
        echo "STATUS:vm_skipped"
        echo "HOSTNAME:${hostname:-$ip}"
        return 2
    fi

    # --- Check sudo ---
    if ! "${SSH[@]}" "sudo -n true" &>/dev/null; then
        echo "[FAIL] Cannot sudo without password on $ip"
        echo "STATUS:sudo_failed"
        echo "HOSTNAME:${hostname:-$ip}"
        return 1
    fi

    # --- Dry run stops here ---
    if [[ "$DRY_RUN" == "true" ]]; then
        local notes=""
        [[ "$use_tunnel" == "true" ]] && notes=" [tunnel]"
        [[ -n "$proxy_cmd" ]] && notes="${notes} [jump]"
        echo "[OK] Would deploy to ${hostname:-$ip} ($SSH_USER, key=$(basename "$ssh_key"))${notes}"
        echo "STATUS:dry_run_ok"
        echo "HOSTNAME:${hostname:-$ip}"
        return 0
    fi

    # --- Step 1: Create remote directories ---
    local remote_agent_dir="$REMOTE_AGENT_DIR"
    local remote_config_dir="$REMOTE_CONFIG_DIR"
    local remote_config="${remote_config_dir}/config.yml"

    echo "[INFO] Creating remote directories..."
    if ! "${SSH[@]}" "sudo mkdir -p $REMOTE_AGENT_DIR $REMOTE_CONFIG_DIR" 2>/dev/null; then
        remote_agent_dir="/var/lib/netbox-agent-test"
        remote_config_dir="/var/lib/netbox-agent-test-config"
        remote_config="${remote_config_dir}/config.yml"
        echo "[WARN] /opt read-only, using $remote_agent_dir"
        if ! "${SSH[@]}" "sudo mkdir -p $remote_agent_dir $remote_config_dir" 2>/dev/null; then
            echo "[FAIL] Cannot create directories"
            echo "STATUS:dir_failed"
            echo "HOSTNAME:${hostname:-$ip}"
            return 1
        fi
    fi

    # --- Step 2: Upload tarball ---
    echo "[INFO] Uploading agent code..."
    if ! "${SCP[@]}" "$TARBALL" "$SSH_USER@$ip:/tmp/netbox-agent-test.tar.gz" &>/dev/null; then
        echo "[FAIL] SCP upload failed"
        echo "STATUS:scp_failed"
        echo "HOSTNAME:${hostname:-$ip}"
        return 1
    fi

    # --- Step 3: Extract and install Python deps ---
    echo "[INFO] Installing agent..."
    ssh "${EXTRA_SSH_OPTS[@]}" -i "$ssh_key" "$SSH_USER@$ip" sh -s "$remote_agent_dir" <<'REMOTE_INSTALL'
set -e
AGENT_DIR="$1"

sudo rm -rf "$AGENT_DIR"
sudo mkdir -p /tmp/netbox-agent-extract
sudo tar xzf /tmp/netbox-agent-test.tar.gz -C /tmp/netbox-agent-extract
sudo mkdir -p "$AGENT_DIR"
if [ -d /tmp/netbox-agent-extract/netbox-agent ]; then
    sudo cp -a /tmp/netbox-agent-extract/netbox-agent/* "$AGENT_DIR/"
fi
sudo rm -rf /tmp/netbox-agent-extract

# Find pip — try multiple strategies for different distros
PIP=""
for p in pip3 pip; do
    if command -v "$p" >/dev/null 2>&1; then PIP="$p"; break; fi
done

# Check if python3 -m pip works (common on RHEL/modern distros)
if [ -z "$PIP" ] && python3 -m pip --version >/dev/null 2>&1; then
    PIP="python3 -m pip"
fi

if [ -z "$PIP" ]; then
    echo "WARN: No pip found — bootstrapping..."
    # Try ensurepip first (works on RHEL 10, most modern Python)
    sudo python3 -m ensurepip --upgrade 2>&1 || true
    # Then try package manager
    if command -v apt-get >/dev/null 2>&1; then
        sudo apt-get install -y -qq python3-pip 2>/dev/null || true
    elif command -v dnf >/dev/null 2>&1; then
        sudo dnf install -y -q python3-pip 2>/dev/null || true
    fi
    # Re-check — use sudo since ensurepip installs to root's local bin
    for p in pip3 pip; do
        if sudo "$p" --version >/dev/null 2>&1; then PIP="$p"; break; fi
    done
    # Fallback to python3 -m pip (also check with sudo)
    if [ -z "$PIP" ] && sudo python3 -m pip --version >/dev/null 2>&1; then
        PIP="python3 -m pip"
    fi
fi

if [ -n "$PIP" ]; then
    cd "$AGENT_DIR"
    sudo $PIP install --break-system-packages --ignore-installed -r requirements.txt 2>&1 || \
    sudo $PIP install --break-system-packages -r requirements.txt 2>&1 || \
    sudo $PIP install -r requirements.txt 2>&1 || true
else
    echo "ERROR: pip not available after all attempts"
fi

rm -f /tmp/netbox-agent-test.tar.gz
REMOTE_INSTALL

    if [[ $? -ne 0 ]]; then
        echo "[FAIL] Installation failed"
        echo "STATUS:install_failed"
        echo "HOSTNAME:${hostname:-$ip}"
        return 1
    fi

    # --- Step 4: Write config ---
    echo "[INFO] Writing config..."
    if [[ "$use_tunnel" == "true" ]]; then
        echo "[INFO] Using tunnel URL ($NETBOX_URL_TUNNEL)"
        generate_config "$NETBOX_URL_TUNNEL" | \
            ssh "${EXTRA_SSH_OPTS[@]}" -i "$ssh_key" "$SSH_USER@$ip" \
            "sudo tee $remote_config > /dev/null"
    else
        generate_config "$NETBOX_URL" | \
            ssh "${EXTRA_SSH_OPTS[@]}" -i "$ssh_key" "$SSH_USER@$ip" \
            "sudo tee $remote_config > /dev/null"
    fi

    # --- Step 5: Install system dependencies ---
    echo "[INFO] Checking system tools..."
    ssh "${EXTRA_SSH_OPTS[@]}" -i "$ssh_key" "$SSH_USER@$ip" sh -s <<'REMOTE_DEPS'
missing=""
for tool in dmidecode ipmitool lshw lsblk lscpu ethtool nvme-cli; do
    check_name="$tool"
    [ "$tool" = "nvme-cli" ] && check_name="nvme"
    if ! command -v $check_name &>/dev/null; then
        missing="$missing $tool"
    fi
done
if [ -n "$missing" ]; then
    echo "WARN: Missing tools:$missing — installing..."
    if command -v apt-get &>/dev/null; then
        sudo apt-get install -y -qq $missing 2>/dev/null || true
    elif command -v dnf &>/dev/null; then
        sudo dnf install -y -q $missing 2>/dev/null || true
    elif command -v zypper &>/dev/null; then
        sudo zypper install -y -q $missing 2>/dev/null || true
    fi
fi
REMOTE_DEPS

    # --- Step 6: Install systemd timers ---
    if [[ "$use_tunnel" == "true" ]]; then
        echo "[WARN] Skipping systemd timers — host requires SSH tunnel to reach NetBox"
        echo "[WARN] Timers would fail without a persistent tunnel. Manual runs via orchestrator only."
    else
    echo "[INFO] Installing systemd timers..."
    ssh "${EXTRA_SSH_OPTS[@]}" -i "$ssh_key" "$SSH_USER@$ip" sh -s "$remote_agent_dir" "$remote_config" <<'REMOTE_SYSTEMD'
set -e
AGENT_DIR="$1"
CONFIG_FILE="$2"
SYSTEMD_DIR="/etc/systemd/system"

# Template the service files — replace hardcoded paths with actual install dir
for src in "$AGENT_DIR/scripts/systemd/"*.service "$AGENT_DIR/scripts/systemd/"*.timer; do
    [ -f "$src" ] || continue
    fname=$(basename "$src")
    # Replace /opt/netbox-agent-test with actual AGENT_DIR
    sudo sed -e "s|/opt/netbox-agent-test|$AGENT_DIR|g" \
             -e "s|/etc/netbox-agent-test/config.yml|$CONFIG_FILE|g" \
             "$src" | sudo tee "$SYSTEMD_DIR/$fname" > /dev/null
    echo "  Installed: $fname"
done

# Reload systemd, enable timers (not boot service — enable separately if desired)
sudo systemctl daemon-reload 2>/dev/null || true
for timer in netbox-agent-test-daily.timer netbox-agent-test-network.timer; do
    if [ -f "$SYSTEMD_DIR/$timer" ]; then
        sudo systemctl enable "$timer" 2>/dev/null || true
        sudo systemctl start "$timer" 2>/dev/null || true
        echo "  Enabled: $timer"
    fi
done

# Enable boot service
if [ -f "$SYSTEMD_DIR/netbox-agent-test-boot.service" ]; then
    sudo systemctl enable netbox-agent-test-boot.service 2>/dev/null || true
    echo "  Enabled: netbox-agent-test-boot.service"
fi

echo "[OK] Systemd timers installed"
REMOTE_SYSTEMD
    fi  # end if not tunnel

    # --- Step 7: Run the agent ---
    echo "[INFO] Running netbox-agent --update-all ..."
    local output
    if [[ "$use_tunnel" == "true" ]]; then
        echo "[INFO] Opening SSH tunnel (remote:$TUNNEL_PORT → NetBox:443)"
        output=$(ssh -n "${EXTRA_SSH_OPTS[@]}" -i "$ssh_key" \
            -R "${TUNNEL_PORT}:127.0.0.1:443" \
            "$SSH_USER@$ip" \
            "sudo PYTHONPATH=$remote_agent_dir python3 -m netbox_agent.cli -c $remote_config --update-all 2>&1" \
            2>/dev/null) || true
    else
        output=$("${SSH[@]}" \
            "sudo PYTHONPATH=$remote_agent_dir python3 -m netbox_agent.cli -c $remote_config --update-all 2>&1" \
            2>/dev/null) || true
    fi

    echo "$output"
    echo ""

    # Check for fatal errors (ignore known non-critical warnings)
    local filtered
    filtered=$(echo "$output" | grep -vi \
        -e "Cannot report power consumption" \
        -e "IPMI decoding failed" \
        -e "pip.*root.*user" \
        -e "WARNING:.*Running pip" \
        -e "error: externally-managed-environment" \
        -e "KeyError.*Lenovo" || true)

    if echo "$filtered" | grep -qi "exception\|traceback"; then
        echo "[FAIL] Agent ran with errors"
        echo "STATUS:agent_error"
    else
        echo "[OK] Agent completed successfully"
        echo "STATUS:success"
    fi
    echo "HOSTNAME:${hostname:-$ip}"
    return 0
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    mkdir -p "$LOG_DIR"

    echo ""
    echo -e "${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║          netbox-agent orchestrator                          ║${NC}"
    echo -e "${BOLD}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    log "Targets:     ${#IPS[@]} hosts"
    log "SSH keys:    ${SSH_KEYS[*]##*/}"
    log "User:        $SSH_USER"
    log "Parallel:    $MAX_PARALLEL"
    log "Dry run:     $DRY_RUN"
    log "Logs:        $LOG_DIR/"
    echo ""

    # Build tarball once (skip for dry run)
    if [[ "$DRY_RUN" != "true" ]]; then
        build_tarball
    fi

    # Launch parallel deployments
    declare -A PIDS
    local running=0

    for ip in "${IPS[@]}"; do
        # Throttle: wait for a slot if at capacity
        while (( running >= MAX_PARALLEL )); do
            # Wait for any one job to finish
            wait -n 2>/dev/null || true
            # Recount running jobs
            running=0
            for pid in "${PIDS[@]}"; do
                kill -0 "$pid" 2>/dev/null && ((running++)) || true
            done
        done

        log "Launching: $ip"
        deploy_host "$ip" &
        PIDS["$ip"]=$!
        ((running++))
    done

    # Wait for all remaining jobs
    log "Waiting for ${#PIDS[@]} deployments to complete..."
    echo ""
    wait

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    local total=0 success=0 failed=0 skipped=0 auth_fail=0 dry_ok=0

    echo ""
    echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"
    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "${BOLD}  DRY RUN RESULTS${NC}"
    else
        echo -e "${BOLD}  DEPLOYMENT RESULTS${NC}"
    fi
    echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"
    printf "  ${BOLD}%-18s %-25s %s${NC}\n" "IP" "HOSTNAME" "STATUS"
    echo "  ---------------------------------------------------------------"

    for ip in "${IPS[@]}"; do
        ((total++))
        local logfile="$LOG_DIR/${ip}.log"
        local status="unknown"
        local hostname="$ip"

        if [[ -f "$logfile" ]]; then
            status=$(grep "^STATUS:" "$logfile" | tail -1 | cut -d: -f2-)
            local h
            h=$(grep "^HOSTNAME:" "$logfile" | tail -1 | cut -d: -f2-)
            [[ -n "$h" ]] && hostname="$h"
        fi

        case "$status" in
            success)
                printf "  ${GREEN}%-18s %-25s ✓ success${NC}\n" "$ip" "$hostname"
                ((success++)) ;;
            dry_run_ok)
                printf "  ${GREEN}%-18s %-25s ✓ reachable${NC}\n" "$ip" "$hostname"
                ((dry_ok++)) ;;
            vm_skipped)
                printf "  ${YELLOW}%-18s %-25s ⊘ VM skipped${NC}\n" "$ip" "$hostname"
                ((skipped++)) ;;
            auth_failed)
                printf "  ${RED}%-18s %-25s ✗ SSH auth failed${NC}\n" "$ip" "$hostname"
                ((auth_fail++)) ;;
            sudo_failed)
                printf "  ${RED}%-18s %-25s ✗ sudo failed${NC}\n" "$ip" "$hostname"
                ((failed++)) ;;
            agent_error)
                printf "  ${YELLOW}%-18s %-25s ⚠ agent errors${NC}\n" "$ip" "$hostname"
                ((failed++)) ;;
            install_failed|scp_failed|dir_failed)
                printf "  ${RED}%-18s %-25s ✗ ${status}${NC}\n" "$ip" "$hostname"
                ((failed++)) ;;
            *)
                printf "  ${RED}%-18s %-25s ✗ ${status}${NC}\n" "$ip" "$hostname"
                ((failed++)) ;;
        esac
    done

    echo ""
    echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"
    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "  Total:        $total"
        echo -e "  Reachable:    ${GREEN}$dry_ok${NC}"
        echo -e "  Auth failed:  ${RED}$auth_fail${NC}"
        echo -e "  VM skipped:   ${YELLOW}$skipped${NC}"
    else
        echo -e "  Total:        $total"
        echo -e "  ${GREEN}Success:      $success${NC}"
        echo -e "  ${RED}Failed:       $failed${NC}"
        echo -e "  ${YELLOW}Skipped:      $skipped${NC}"
        echo -e "  Auth failed:  $auth_fail"
    fi
    echo -e "  Logs:         $LOG_DIR/"
    echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"

    # Show failed host logs hint
    if (( failed > 0 )); then
        echo ""
        echo "  View failed host logs:"
        for ip in "${IPS[@]}"; do
            local s
            s=$(grep "^STATUS:" "$LOG_DIR/${ip}.log" 2>/dev/null | tail -1 | cut -d: -f2-)
            if [[ "$s" != "success" && "$s" != "dry_run_ok" && "$s" != "vm_skipped" ]]; then
                echo "    cat $LOG_DIR/${ip}.log"
            fi
        done
    fi

    # Cleanup tarball
    [[ -f "$TARBALL" ]] && rm -f "$TARBALL"

    # Exit with failure if any hosts failed
    (( failed > 0 )) && return 1
    return 0
}

main

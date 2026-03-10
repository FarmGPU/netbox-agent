# netbox-agent

Hardware inventory agent for [NetBox](https://github.com/netbox-community/netbox). Runs on each host, discovers hardware (CPU, GPU, RAM, NIC, SSD, PSU), network interfaces, and IPs, then syncs everything to NetBox via the REST API.

Forked from [Solvik/netbox-agent](https://github.com/Solvik/netbox-agent). This fork adds the Modules API, ARP neighbor reporting, state-based diff sync, systemd timer deployment, and RHEL bootc support.

---

## Features

- **Module-based hardware inventory** — CPU, GPU, RAM, NIC, SSD, PSU tracked as NetBox Modules (new Modules API)
- **Network interface sync** — physical, bonding, and VLAN interfaces with IPv4/IPv6 addresses
- **IPMI/OOB interface** — creates management interface with OOB IP
- **Asset tag reading** — reads from DMI, IPMI FRU, or custom command
- **ARP neighbor reporting** — scans local network for MAC→IP pairs, POSTs to bmc-api for reconciliation
- **State-based diff sync** — tracks hardware state between runs, only syncs what changed
- **Systemd timers** — daily full sync + 4-hour network sync + boot sync
- **Multi-vendor support** — Dell, HP/HPE, Supermicro, QCT, plus generic fallback
- **Blade server support** — chassis/blade hierarchy, slot detection, GPU expansion handling
- **Platform detection** — auto-detects Linux distribution, sets NetBox platform
- **RHEL bootc support** — works on immutable-root bootc containers via `/var/opt` paths

---

## Requirements

- NetBox >= 3.7
- Python >= 3.8

### System packages

| Package | Purpose |
|---------|---------|
| `ethtool` | NIC speed/type detection |
| `dmidecode` | Hardware identity (serial, manufacturer, model) |
| `ipmitool` | IPMI/BMC interface and asset tag reading |
| `lshw` | Hardware enumeration (GPU, NIC, storage, memory) |
| `arp-scan` | ARP neighbor discovery (optional — falls back to `ip neigh`) |

### Python dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `pynetbox` | 7.4.1 | NetBox API client |
| `netaddr` | 1.3.0 | IP address handling |
| `netifaces2` | 0.0.22 | Network interface enumeration |
| `pyyaml` | 6.0.2 | Configuration parsing |
| `jsonargparse` | 4.36.0 | CLI argument + config file parsing |
| `python-slugify` | 8.0.4 | Slug generation for NetBox |
| `packaging` | 24.2 | Version comparison |
| `distro` | 1.9.0 | Linux distribution detection |

---

## Installation

### Via Ansible role (recommended)

The `netbox-agent` Ansible role handles cloning, venv creation, config deployment, and systemd timer setup. See `fgpu_ansible/roles/netbox-agent/`.

```bash
ansible-playbook test-netbox-agent.yml --limit ginger04 -e netbox_token=<token>
```

### Manual

```bash
git clone git@github.com:FarmGPU/netbox-agent.git
cd netbox-agent
python3 -m venv venv
source venv/bin/activate
pip install -e .
cp netbox_agent.yaml.example /etc/netbox-agent/config.yaml
# Edit config with your NetBox URL and token
```

---

## Usage

```bash
# Full hardware + network sync (first run or daily)
netbox_agent -c /etc/netbox-agent/config.yaml --update-all

# Network-only sync (every 4 hours via timer)
netbox_agent -c /etc/netbox-agent/config.yaml --network-only

# ARP neighbor scan and report to bmc-api
netbox_agent -c /etc/netbox-agent/config.yaml --arp-report

# Register a new device (create in NetBox)
netbox_agent -c /etc/netbox-agent/config.yaml --register
```

### CLI flags

| Flag | Description |
|------|-------------|
| `-c, --config` | Path to config file |
| `-r, --register` | Create new device in NetBox |
| `-u, --update-all` | Full sync: hardware, network, location, PSU |
| `--update-network` | Sync network interfaces and IPs only |
| `--update-inventory` | Sync legacy inventory items only |
| `--update-location` | Sync datacenter/rack location only |
| `--update-psu` | Sync power supplies only |
| `--network-only` | Skip hardware, sync network only (fast) |
| `--arp-report` | Run ARP scan and POST pairs to bmc-api |
| `--modules` | Enable Modules API hardware inventory |
| `--update-modules` | Update modules this run |
| `--expansion-as-device` | Treat blade expansions as separate devices |
| `-d, --debug` | Enable debug logging |
| `--log_level` | Set log level (default: debug) |
| `--state-dir` | State file directory (default: `/var/lib/netbox-agent`) |

---

## Configuration

Configuration is loaded from YAML file, environment variables (`NETBOX_AGENT_` prefix), or CLI flags. Precedence: CLI > env vars > config file > defaults.

See [`netbox_agent.yaml.example`](netbox_agent.yaml.example) for a complete reference.

### Minimal config

```yaml
netbox:
  url: 'https://10.100.248.18'
  token: your-netbox-token
  ssl_verify: false

network:
  ignore_interfaces: "(dummy.*|docker.*)"
  ignore_ips: "(127\\.0\\.0\\..*)"

datacenter_location:
  driver: "cmd:echo datacenter: smf01"
  regex: "datacenter: (?P<datacenter>[A-Za-z0-9]+)"

# Hardware inventory via Modules API
modules: true
update_modules: true
inventory: false

# Spare device for re-parenting removed hardware
spare_device_name: "SPARE-INVENTORY"
```

### ARP reporting config

```yaml
arp_report:
  enabled: true
  bmc_api_url: "http://10.100.248.18:8100"
  bmc_api_key: "sk-operator-def456"
  interfaces: ""         # empty = scan all non-ignored interfaces
  scan_timeout: 30       # seconds per interface for arp-scan subprocess
```

### Device config

```yaml
device:
  server_role: "Server"
  default_owner: "FarmGPU"
  asset_tag_cmd: "dmidecode -s chassis-asset-tag"
  tags: ""
  custom_fields: ""
```

### Location drivers

```yaml
# Static site (single datacenter)
datacenter_location:
  driver: "cmd:echo datacenter: smf01"
  regex: "datacenter: (?P<datacenter>[A-Za-z0-9]+)"

# From LLDP switch name
# datacenter_location:
#   driver: "cmd:lldpctl"
#   regex: "SysName: .*\\.([A-Za-z0-9]+)"

# Rack from LLDP
# rack_location:
#   driver: "cmd:lldpctl"
#   regex: "SysName:[ ]+[A-Za-z]+-[A-Za-z]+-([A-Za-z0-9]+)"
```

---

## Hardware Modules

The Modules API (`--modules`) replaces the legacy Inventory Items approach. Each hardware component is tracked as a NetBox Module with its own serial number, manufacturer, and module type.

### Supported module types

| Category | Detection | Serial Source | Example |
|----------|-----------|---------------|---------|
| **CPU** | `lscpu -J` (primary), lshw fallback | None (positional match) | Intel Xeon w9-3545X |
| **GPU** | lshw, `nvidia-smi` for serials | nvidia-smi query | NVIDIA H200 |
| **RAM** | lshw memory children | DMI serial | Samsung M321R8GA0PB0-CXYZZ |
| **SSD/NVMe** | `lsblk -J`, `nvme list` enrichment | Drive serial | Micron MTFDKBA3T8TFH |
| **NIC** | lshw network | MAC address | Broadcom BCM57504 |
| **PSU** | DMI type 39 | PSU serial | PWS-2K26A-1R |

### Sync algorithm

1. Detect current hardware via system tools
2. Compare against last known state (`state.py` diff)
3. For each module type:
   - Match by serial number (handles hardware moves between servers)
   - Positional match for items without serials (CPUs)
   - Create new modules for unrecognized hardware
   - Move removed hardware to `SPARE-INVENTORY` device

### Spare device

When hardware is removed from a server (e.g., a GPU is pulled), the module is re-parented to the `SPARE-INVENTORY` device rather than deleted. This preserves the serial number history and makes it easy to track where hardware went.

---

## ARP Neighbor Reporting

The ARP reporter scans the local network for MAC→IP pairs and POSTs them to bmc-api for reconciliation against NetBox. This catches stale BMC IPs on subnets without DHCP.

### How it works

```
1. Agent determines interfaces to scan
   - Explicit list from config, or auto-detect (UP + has IPv4 + not ignored)

2. Scan each interface
   - Primary: arp-scan --localnet --interface={iface} --plain
   - Fallback: ip -j neigh show (REACHABLE entries only)

3. Deduplicate pairs (same MAC → keep last IP seen)

4. POST to bmc-api:
   POST http://bmc-api:8100/arp-pairs
   {"pairs": [{"mac": "3C:EC:EF:C8:DF:4B", "ip": "10.100.192.50"}, ...],
    "hostname": "ginger04"}

5. bmc-api reconciles against NetBox
```

### arp-scan vs ip neigh

| Method | Pros | Cons |
|--------|------|------|
| `arp-scan` | Active scan, discovers all L2 neighbors | Requires arp-scan package, needs root |
| `ip neigh` | Zero dependencies, always available | Passive — only sees hosts this machine has talked to recently |

The agent uses arp-scan when available and falls back to `ip neigh show` automatically. On RHEL/bootc hosts where arp-scan isn't in the repos, the fallback is used.

---

## State-Based Sync

The agent tracks hardware and network state between runs in a JSON file (`/var/lib/netbox-agent/last_state.json`). On subsequent runs, it diffs the current state against the saved state and only syncs categories that changed.

```
First run:   full sync (no state file)
Later runs:  diff → only sync changed categories
```

State includes:
- Hardware: CPU, GPU, DIMM, SSD, NIC, PSU (keyed by serial or product+vendor)
- Network: interface names, IP addresses
- Dependencies: which system tools are available

Atomic writes (temp file + rename) prevent corruption from concurrent runs or crashes.

---

## Systemd Deployment

The Ansible role deploys three services and two timers:

| Unit | Schedule | Action |
|------|----------|--------|
| `netbox-agent-boot.service` | On boot (60s delay) | `--update-all` |
| `netbox-agent-daily.timer` | Daily 3:00 AM (±5min jitter) | `--update-all` |
| `netbox-agent-network.timer` | Every 4 hours (±2min jitter) | `--network-only` |

The boot service ensures new/rebooted hosts register immediately. The daily timer catches hardware changes. The network timer keeps IPs current.

---

## Device Sync Flow

When the agent runs `--update-all`:

```
1. Read DMI data (dmidecode)
   → manufacturer, model, serial, chassis serial, asset tag

2. Detect vendor class (Dell, HP, Supermicro, QCT, Generic)

3. Find or create device in NetBox:
   a. Search by asset tag (highest priority)
   b. Search by serial number
   c. Search by BMC MAC (custom field)
   d. Create new if not found

4. Set device fields:
   → platform (auto-detected Linux distro)
   → custom fields: owner, environment, chassis_serial, bmc_mac_address

5. Sync hardware modules (if --modules):
   → CPU, GPU, RAM, NIC, SSD, PSU as NetBox Modules

6. Sync network interfaces:
   → physical, bonding, VLAN interfaces
   → IPv4 and IPv6 addresses
   → primary IP (interface with default gateway)
   → OOB IP (IPMI interface)

7. Sync PSUs:
   → power ports with max power ratings

8. Run ARP report (if arp_report.enabled):
   → scan + POST to bmc-api

9. Save state for next diff
```

---

## RHEL bootc Support

On immutable-root bootc systems (e.g., TractorOS):

- **Install path**: `/var/opt/netbox-agent` (root filesystem is read-only)
- **Config path**: `/etc/netbox-agent/config.yaml` (`/etc` is writable)
- **Packages**: `dnf install --transient` (survives until reboot)
- **arp-scan**: Not available in RHEL repos — uses `ip neigh` fallback

The Ansible role handles this automatically when `netbox_agent_bootc: true` is set.

---

## File Structure

```
netbox_agent/
├── cli.py               # Entry point, vendor detection, orchestration
├── config.py            # Config parsing (YAML + env + CLI args)
├── server.py            # Device create/update, NetBox sync orchestration
├── modules.py           # Modules API: CPU, GPU, RAM, NIC, SSD, PSU
├── network.py           # Network interface and IP sync
├── power.py             # PSU detection and power port management
├── arp_reporter.py      # ARP scan + POST to bmc-api
├── state.py             # State-based diff tracking between runs
├── inventory.py         # Legacy inventory items (deprecated)
├── dmidecode.py         # DMI data parsing
├── lshw.py              # lshw JSON parsing
├── ethtool.py           # ethtool output parsing
├── ipmi.py              # IPMI FRU data + asset tag reading
├── lldp.py              # LLDP neighbor parsing for auto-cabling
├── location.py          # Datacenter/rack/slot location drivers
├── hypervisor.py        # VM and hypervisor cluster management
├── logging.py           # Logging setup
├── misc.py              # Utility functions
├── dependencies.py      # System dependency detection
├── drivers/
│   ├── cmd.py           # Shell command driver
│   └── file.py          # File-based driver
├── vendors/
│   ├── dell.py          # Dell PowerEdge specifics
│   ├── hp.py            # HP/HPE ProLiant specifics
│   ├── supermicro.py    # Supermicro specifics
│   ├── qct.py           # QCT QuantaMicro specifics
│   └── generic.py       # Fallback for unknown vendors
└── raid/
    ├── base.py          # RAID controller base class
    ├── hp.py            # HP Smart Array (hpssacli)
    ├── storcli.py       # Broadcom MegaRAID (storcli)
    └── omreport.py      # Dell OMSA (omreport)
```

---

## Tested Hardware

### FarmGPU fleet

| Codename | Model | Vendor | Count | Notes |
|-----------|-------|--------|-------|-------|
| Ginger | SSG-222B-NE3X24R | Supermicro | 6 | VAST storage, 24× NVMe |
| Potato | SYS-212H-TN | Supermicro | 7 | MinIO Exapod, RHEL bootc |

### Upstream (Solvik)

- Dell: PowerEdge MX7000, M1000e, MX740c, M640, M630, M620, M610, DSS7500
- HP/HPE: BladeSystem c7000, ProLiant BL460c Gen8-10, Moonshot 1500, DL380p, SL4540, XL450
- Supermicro: SBI/SBA blades, SSG-6028R, SYS-6018R
- QCT: QuantaMicro X10E-9N
- VMs: Hyper-V, VMWare, VirtualBox, AWS, GCP

---

## Development

```bash
git clone git@github.com:FarmGPU/netbox-agent.git
cd netbox-agent
git checkout ethan
python3 -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
```

### Linting

```bash
ruff check
ruff format
```

### Testing

```bash
pytest tests/ -v
```

---

## Known Limitations

- Linux only (uses `ethtool`, `/sys/` parsing)
- Requires root access for `dmidecode`, `ipmitool`, `arp-scan`
- LLDP auto-cabling requires `lldpd` running (disabled by default)
- Legacy inventory items (`inventory: true`) are deprecated — use `modules: true`
- CPU modules lack serial numbers — matched by position only
- `ip neigh` fallback only discovers hosts with recent REACHABLE ARP entries

---

## License

Apache-2.0 — see [LICENSE](LICENSE)

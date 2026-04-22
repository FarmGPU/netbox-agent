import sys

# Handle --version before any imports that trigger argparse
if "--version" in sys.argv or "-V" in sys.argv:
    from netbox_agent import __version__
    print(f"netbox-agent {__version__}")
    sys.exit(0)

from packaging import version
import netbox_agent.dmidecode as dmidecode
from netbox_agent.config import config
from netbox_agent.config import netbox_instance as nb
from netbox_agent.dependencies import log_status as log_deps
from netbox_agent.logging import logging  # NOQA
from netbox_agent.state import StateManager
from netbox_agent.vendors.dell import DellHost
from netbox_agent.vendors.generic import GenericHost
from netbox_agent.vendors.hp import HPHost
from netbox_agent.vendors.qct import QCTHost
from netbox_agent.vendors.supermicro import SupermicroHost
from netbox_agent.virtualmachine import VirtualMachine, is_vm

MANUFACTURERS = {
    "Dell Inc.": DellHost,
    "HP": HPHost,
    "HPE": HPHost,
    "Supermicro": SupermicroHost,
    "Quanta Cloud Technology Inc.": QCTHost,
    "Generic": GenericHost,
}


def run(config):
    # Pre-flight dependency check
    deps = log_deps()

    # Initialize state manager for diff-based sync
    state_dir = getattr(config, "state_dir", "/var/lib/netbox-agent-test")
    state = StateManager(state_dir)

    dmi = dmidecode.parse()

    if config.virtual.enabled or is_vm(dmi):
        if config.virtual.hypervisor:
            raise Exception("This host can't be a hypervisor because it's a VM")
        if not config.virtual.cluster_name:
            logging.warning(
                "This host is a virtual machine (not a physical server). "
                "Skipping — set virtual.cluster_name in config to register VMs."
            )
            return 0
        server = VirtualMachine(dmi=dmi)
    else:
        if config.virtual.hypervisor and not config.virtual.cluster_name:
            raise Exception(
                "virtual.cluster_name parameter is mandatory because it's a hypervisor"
            )
        manufacturer = dmidecode.get_by_type(dmi, "Chassis")[0].get("Manufacturer")
        try:
            server = MANUFACTURERS[manufacturer](dmi=dmi)
        except KeyError:
            server = GenericHost(dmi=dmi)

    if version.parse(nb.version) < version.parse("3.7"):
        print("netbox-agent is not compatible with Netbox prior to version 3.7")
        return 1

    network_only = getattr(config, "network_only", False)

    if (
        config.register
        or config.update_all
        or config.update_network
        or config.update_location
        or config.update_inventory
        or config.update_psu
        or getattr(config, "update_modules", False)
        or network_only
    ):
        server.netbox_create_or_update(
            config, deps=deps, network_only=network_only, state=state,
        )
    if config.debug:
        server.print_debug()

    # ARP neighbor reporting (after main sync)
    arp_enabled = (
        getattr(config, "arp_report", None)
        and getattr(config.arp_report, "enabled", False)
    )
    arp_flag = getattr(config, "arp_report_flag", False)
    if arp_enabled or arp_flag:
        from netbox_agent.arp_reporter import scan_and_report
        try:
            arp_result = scan_and_report(config)
            logging.info(
                "ARP report: %d pairs found, %d submitted from %d interfaces (%s)",
                arp_result["pairs_found"],
                arp_result["pairs_submitted"],
                arp_result["interfaces_scanned"],
                arp_result["method"],
            )
        except Exception as e:
            logging.warning("ARP report failed (non-fatal): %s", e)

    return 0


def main():
    return run(config)


if __name__ == "__main__":
    sys.exit(main())

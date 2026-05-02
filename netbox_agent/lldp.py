import logging
import subprocess

from netbox_agent.misc import is_tool


class LLDP:
    def __init__(self, output=None):
        if not is_tool("lldpctl"):
            # Promoted from debug to warning: lldpd is the source of truth
            # for per-iface (per-bond-slave) cabling; without it bonded
            # NICs will not be cabled to their switch ports (INF-320).
            logging.warning(
                "lldpd / lldpctl not found — bonded NIC slaves will not be "
                "cabled in NetBox. Install with `apt install lldpd && "
                "systemctl enable --now lldpd`."
            )
        if output:
            self.output = output
        else:
            self.output = subprocess.getoutput("lldpctl -f keyvalue")
        self.data = self.parse()

    def parse(self):
        output_dict = {}
        vlans = {}
        vid = None
        for entry in self.output.splitlines():
            if "=" not in entry:
                continue
            path, value = entry.strip().split("=", 1)
            # When a chassis advertises both IPv4 and IPv6 mgmt-ips,
            # lldpctl emits separate `chassis.mgmt-ip=` lines and the
            # parser overwrites — so the LAST one wins. IPv6 link-local
            # (`fe80::*`) is non-addressable for NetBox switch lookup;
            # drop it so the IPv4 mgmt-ip stays as the resolved switch
            # address (INF-320).
            if path.endswith("chassis.mgmt-ip") and value.lower().startswith("fe80"):
                continue
            split_path = path.split(".")
            interface = split_path[1]
            path_components, final = split_path[:-1], split_path[-1]
            current_dict = output_dict

            if vlans.get(interface) is None:
                vlans[interface] = {}

            for path_component in path_components:
                if not isinstance(current_dict.get(path_component), dict):
                    current_dict[path_component] = {}
                current_dict = current_dict.get(path_component)
                if "vlan-id" in path:
                    vid = value
                    vlans[interface][value] = vlans[interface].get(vid, {})
                elif path.endswith("vlan"):
                    vid = value.replace("vlan-", "").replace("VLAN", "")
                    vlans[interface][vid] = vlans[interface].get(vid, {})
                elif "pvid" in path:
                    vlans[interface][vid]["pvid"] = True
            if "vlan" not in path:
                current_dict[final] = value
        for interface, vlan in vlans.items():
            output_dict["lldp"][interface]["vlan"] = vlan
        if not output_dict:
            logging.debug("No LLDP output, please check your network config.")
        return output_dict

    def get_switch_ip(self, interface):
        # lldp.eth0.chassis.mgmt-ip=100.66.7.222
        if self.data.get("lldp", {}).get(interface) is None:
            return None
        return self.data["lldp"][interface]["chassis"].get("mgmt-ip")

    def get_switch_port(self, interface):
        # lldp.eth0.port.descr=GigabitEthernet1/0/1
        if self.data.get("lldp", {}).get(interface) is None:
            return None
        if self.data["lldp"][interface]["port"].get("ifname"):
            return self.data["lldp"][interface]["port"]["ifname"]
        return self.data["lldp"][interface]["port"]["descr"]

    def get_switch_vlan(self, interface):
        # lldp.eth0.vlan.vlan-id=296
        if self.data.get("lldp", {}).get(interface) is None:
            return None
        return self.data["lldp"][interface]["vlan"]

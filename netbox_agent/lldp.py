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
        # Drop non-switch neighbors before parsing. lldpctl's keyvalue format
        # places every neighbor on the same iface under `lldp.<iface>.*`, and
        # parse() overwrites earlier blocks with later ones. On hosts with
        # BlueField-3 / other Smart NICs, the SoC emits its own LLDPDUs
        # (capability=Station) on internal port representors that the host
        # iface picks up. If a Station block arrives after the real switch
        # block, it overwrites it and cabling fails. Keep only Bridge/Router-
        # capable blocks so switches always win regardless of arrival order
        # (INF-318).
        self.output = self._filter_to_switch_neighbors(self.output)
        self.data = self.parse()

    @staticmethod
    def _filter_to_switch_neighbors(output: str) -> str:
        """Keep only LLDP neighbor blocks whose chassis advertises Bridge or Router.

        Each neighbor block in lldpctl's keyvalue format starts with a
        `lldp.<iface>.via=LLDP` line. We accumulate per-block lines, inspect
        each block's `chassis.Bridge.enabled` / `chassis.Router.enabled` fields,
        and drop blocks that are not switch-like.
        """
        kept: list[str] = []
        current: list[str] = []
        is_switch = False

        def flush():
            nonlocal current, is_switch
            if current and is_switch:
                kept.extend(current)
            current = []
            is_switch = False

        for line in output.splitlines():
            if line.endswith(".via=LLDP"):
                flush()
            current.append(line)
            if line.endswith("chassis.Bridge.enabled=on") or line.endswith(
                "chassis.Router.enabled=on"
            ):
                is_switch = True
        flush()
        return "\n".join(kept)

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

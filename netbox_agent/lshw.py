from netbox_agent.misc import is_tool
import subprocess
import logging
import json


class LSHW:
    def __init__(self):
        self.hw_info = {}
        self.info = {}
        self.memories = []
        self.interfaces = []
        self.cpus = []
        self.power = []
        self.disks = []
        self.gpus = []
        self.vendor = "Unknown"
        self.product = "Unknown"
        self.chassis_serial = "Unknown"
        self.motherboard_serial = "No S/N"
        self.motherboard = "Motherboard"

        if not is_tool("lshw"):
            logging.warning("lshw not found -- hardware tree unavailable")
            return

        data = subprocess.getoutput("lshw -quiet -json")
        json_data = json.loads(data)
        # Starting from version 02.18, `lshw -json` wraps its result in a list
        # rather than returning directly a dictionary
        if isinstance(json_data, list):
            self.hw_info = json_data[0]
        else:
            self.hw_info = json_data

        self.vendor = self.hw_info.get("vendor", "Unknown")
        self.product = self.hw_info.get("product", "Unknown")
        self.chassis_serial = self.hw_info.get("serial", "Unknown")
        children = self.hw_info.get("children", [])
        if children:
            self.motherboard_serial = children[0].get("serial", "No S/N")
            self.motherboard = children[0].get("product", "Motherboard")

        for k in children:
            if k.get("class") == "power":
                self.power.append(k)

            if "children" in k:
                for j in k["children"]:
                    if j["class"] == "generic":
                        continue

                    if j["class"] == "storage":
                        self.find_storage(j)

                    if j["class"] == "memory":
                        self.find_memories(j)

                    if j["class"] == "processor":
                        self.find_cpus(j)

                    if j["class"] == "bridge":
                        self.walk_bridge(j)

    def get_hw_linux(self, hwclass):
        if hwclass == "cpu":
            return self.cpus
        if hwclass == "gpu":
            return self.gpus
        if hwclass == "network":
            return self.interfaces
        if hwclass == "storage":
            return self.disks
        if hwclass == "memory":
            return self.memories

    def find_network(self, obj):
        # Some interfaces do not have device (logical) name (eth0, for
        # instance), such as not connected network mezzanine cards in blade
        # servers. In such situations, the card will be named `unknown[0-9]`.
        unkn_intfs = []
        for i in self.interfaces:
            # newer versions of lshw can return a list of names, see issue #227
            if not isinstance(i["name"], list):
                if i["name"].startswith("unknown"):
                    unkn_intfs.append(i)
            else:
                for j in i["name"]:
                    if j.startswith("unknown"):
                        unkn_intfs.append(j)

        unkn_name = "unknown{}".format(len(unkn_intfs))
        self.interfaces.append(
            {
                "name": obj.get("logicalname", unkn_name),
                "macaddress": obj.get("serial", ""),
                "serial": obj.get("serial", ""),
                "product": obj.get("product", "Unknown NIC"),
                "vendor": obj.get("vendor", "Unknown"),
                "description": obj.get("description", ""),
            }
        )

    def find_storage(self, obj):
        if "children" in obj:
            for device in obj["children"]:
                self.disks.append(
                    {
                        "logicalname": device.get("logicalname"),
                        "product": device.get("product"),
                        "serial": device.get("serial"),
                        "version": device.get("version"),
                        "size": device.get("size"),
                        "description": device.get("description"),
                        "type": device.get("description"),
                    }
                )
        elif "driver" in obj["configuration"] and "nvme" in obj["configuration"]["driver"]:
            if not is_tool("nvme"):
                logging.error("nvme-cli >= 1.0 does not seem to be installed")
                return
            try:
                nvme = json.loads(
                    subprocess.check_output(["nvme", "-list", "-o", "json"], encoding="utf8")
                )
                for device in nvme["Devices"]:
                    d = {
                        "logicalname": device["DevicePath"],
                        "product": device["ModelNumber"],
                        "serial": device["SerialNumber"],
                        "version": device["Firmware"],
                        "description": "NVME",
                        "type": "NVME",
                    }
                    if "UsedSize" in device:
                        d["size"] = device["UsedSize"]
                    if "UsedBytes" in device:
                        d["size"] = device["UsedBytes"]
                    self.disks.append(d)
            except Exception:
                pass

    def find_cpus(self, obj):
        if "product" in obj:
            self.cpus.append(
                {
                    "product": obj.get("product", "Unknown CPU"),
                    "vendor": obj.get("vendor", "Unknown vendor"),
                    "description": obj.get("description", ""),
                    "location": obj.get("slot", ""),
                }
            )

    def find_memories(self, obj):
        if "children" not in obj:
            # print("not a DIMM memory.")
            return

        for dimm in obj["children"]:
            if "empty" in dimm["description"]:
                continue

            self.memories.append(
                {
                    "slot": dimm.get("slot"),
                    "description": dimm.get("description"),
                    "id": dimm.get("id"),
                    "serial": dimm.get("serial", "N/A"),
                    "vendor": dimm.get("vendor", "N/A"),
                    "product": dimm.get("product", "N/A"),
                    "size": dimm.get("size", 0) / 2**20 / 1024,
                }
            )

    def find_gpus(self, obj):
        if "product" in obj:
            infos = {
                "product": obj.get("product", "Unknown GPU"),
                "vendor": obj.get("vendor", "Unknown"),
                "description": obj.get("description", ""),
            }
            self.gpus.append(infos)

    def walk_bridge(self, obj):
        """Recursively walk PCI bridge tree to find all devices."""
        if "children" not in obj:
            return

        for child in obj["children"]:
            cls = child.get("class", "")
            if cls == "storage":
                self.find_storage(child)
            elif cls == "network":
                self.find_network(child)
            elif cls == "display":
                self.find_gpus(child)
            elif cls == "bridge":
                self.walk_bridge(child)

            # Also walk children of non-bridge nodes (e.g., storage controllers
            # with child disks are handled by find_storage, but multi-function
            # PCI devices may have network children under a storage parent).
            if cls not in ("bridge",) and "children" in child:
                for grandchild in child["children"]:
                    gc_cls = grandchild.get("class", "")
                    if gc_cls == "storage":
                        self.find_storage(grandchild)
                    elif gc_cls == "network":
                        self.find_network(grandchild)
                    elif gc_cls == "display":
                        self.find_gpus(grandchild)
                    elif gc_cls == "bridge":
                        self.walk_bridge(grandchild)


if __name__ == "__main__":
    pass

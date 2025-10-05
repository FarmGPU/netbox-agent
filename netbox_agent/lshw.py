from netbox_agent.misc import is_tool
import subprocess
import logging
import json
import sys


class LSHW:
    def __init__(self):
        if not is_tool("lshw"):
            logging.error("lshw does not seem to be installed")
            sys.exit(1)

        data = subprocess.getoutput("lshw -quiet -json")
        json_data = json.loads(data)
        # Starting from version 02.18, `lshw -json` wraps its result in a list
        # rather than returning directly a dictionary
        if isinstance(json_data, list):
            self.hw_info = json_data[0]
        else:
            self.hw_info = json_data
        self.info = {}
        self.memories = []
        self.interfaces = []
        self.cpus = []
        self.power = []
        self.disks = []
        self.gpus = []
        self.vendor = self.hw_info["vendor"]
        self.product = self.hw_info["product"]
        self.chassis_serial = self.hw_info.get("serial", "")
        self.motherboard_serial = self.hw_info["children"][0].get("serial", "No S/N")
        self.motherboard = self.hw_info["children"][0].get("product", "Motherboard")

        for k in self.hw_info["children"]:
            if k["class"] == "power":
                # self.power[k["id"]] = k
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
            # For storage controllers with children, we need to handle two cases:
            # 1. NVMe: parent has metadata, children have logical names
            # 2. SAS/SCSI: children have both metadata and logical names
            
            parent_info = {
                "product": obj.get("product"),
                "vendor": obj.get("vendor"),
                "serial": obj.get("serial"),
                "version": obj.get("version"),
                "description": obj.get("description"),
            }
            
            for device in obj["children"]:
                # Skip non-disk children (like hwmon, ng devices, enclosures)
                if device.get("class") != "disk":
                    continue
                
                # Skip devices without actual disk logical names
                try:
                    logicalname = device.get("logicalname")
                    # Handle both string and list logicalname (e.g., ["/dev/nvme0n1", "/mnt/chunks1"])
                    if isinstance(logicalname, list):
                        # Find the first /dev/ path in the list
                        dev_path = None
                        for path in logicalname:
                            if isinstance(path, str) and path.startswith("/dev/"):
                                dev_path = path
                                break
                        if not dev_path:
                            continue
                        logicalname = dev_path
                    elif not logicalname or not logicalname.startswith("/dev/"):
                        continue
                except:
                    print('!', logicalname)
                    continue
                
                # Skip namespace group devices (ng) - these are not actual disks
                if "/dev/ng" in logicalname:
                    continue
                
                # Check if the child has its own metadata (SAS/SCSI case)
                # If child has product info, use it; otherwise use parent info (NVMe case)
                if device.get("product"):
                    # SAS/SCSI case: child has all the metadata
                    disk_info = {
                        "logicalname": logicalname,
                        "product": device.get("product"),
                        "vendor": device.get("vendor"),
                        "serial": device.get("serial"),
                        "version": device.get("version"),
                        "size": device.get("size"),
                        "description": device.get("description"),
                        "type": device.get("description"),
                    }
                else:
                    # NVMe case: combine parent controller info with child namespace info
                    # Try to get better info from nvme list command if available
                    nvme_info = self._get_nvme_info(logicalname)
                    disk_info = {
                        "logicalname": logicalname,
                        "product": nvme_info.get("product", parent_info["product"]),
                        "vendor": nvme_info.get("vendor", parent_info["vendor"]),
                        "serial": nvme_info.get("serial", parent_info["serial"]),
                        "version": nvme_info.get("version", parent_info["version"]),
                        "size": nvme_info.get("size", device.get("size")),
                        "description": parent_info["description"],
                        "type": parent_info["description"],
                    }
                self.disks.append(disk_info)
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

    def _get_nvme_info(self, device_path):
        """Get NVMe device information using nvme list command"""
        if not is_tool("nvme"):
            return {}
        
        try:
            nvme_output = subprocess.check_output(["nvme", "list", "-o", "json"], encoding="utf8")
            nvme_data = json.loads(nvme_output)
            
            # Find the device in the nvme list output
            for device in nvme_data.get("Devices", []):
                if device.get("DevicePath") == device_path:
                    return {
                        "product": device.get("ModelNumber"),
                        "serial": device.get("SerialNumber"),
                        "version": device.get("Firmware"),
                        "size": device.get("UsedBytes") or device.get("PhysicalSize"),
                        "vendor": "Unknown"  # Will be determined by get_vendor() in inventory.py
                    }
        except Exception:
            pass
        
        return {}

    def walk_bridge(self, obj):
        if "children" not in obj:
            return

        for bus in obj["children"]:
            if bus["class"] == "storage":
                self.find_storage(bus)
            if bus["class"] == "display":
                self.find_gpus(bus)
            if bus["class"] == "network":
                self.find_network(bus)

            # Recursively traverse deeper levels
            if "children" in bus:
                self.walk_bridge(bus)


if __name__ == "__main__":
    pass

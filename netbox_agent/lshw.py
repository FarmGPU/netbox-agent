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
        self.accelerators = []  # Non-GPU compute accelerators (Gaudi, FPGA, DPU, etc.)
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
        if hwclass == "accelerator":
            return self.accelerators
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
                    # Use PhysicalSize for the actual disk capacity
                    # UsedBytes/UsedSize only shows used space, not total capacity
                    if "PhysicalSize" in device:
                        d["size"] = device["PhysicalSize"]
                    elif "UsedSize" in device:
                        d["size"] = device["UsedSize"]
                    elif "UsedBytes" in device:
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

    # Vendors whose "coprocessor" class PCI devices are actually GPUs,
    # not custom accelerators. These get routed to self.gpus, not self.accelerators.
    _GPU_VENDORS = {"habana", "intel", "amd", "nvidia"}

    def find_gpus(self, obj):
        if "product" in obj:
            infos = {
                "product": obj.get("product", "Unknown GPU"),
                "vendor": obj.get("vendor", "Unknown"),
                "description": obj.get("description", ""),
                "businfo": obj.get("businfo", ""),  # PCI bus ID for driver lookup
            }
            self.gpus.append(infos)

    def _get_nvme_info(self, device_path):
        """Get NVMe device information using nvme list command."""
        if not is_tool("nvme"):
            return {}
        try:
            nvme_output = subprocess.check_output(
                ["nvme", "list", "-o", "json"], encoding="utf8"
            )
            nvme_data = json.loads(nvme_output)
            for device in nvme_data.get("Devices", []):
                if device.get("DevicePath") == device_path:
                    return {
                        "product": device.get("ModelNumber"),
                        "serial": device.get("SerialNumber"),
                        "version": device.get("Firmware"),
                        "size": device.get("PhysicalSize") or device.get("UsedBytes"),
                        "vendor": "Unknown",
                    }
        except Exception:
            pass
        return {}

    # Descriptions that indicate chipset infrastructure, NOT real accelerators.
    # These are IOMMU, host bridges, system peripherals, etc. that lshw
    # classifies as "generic" but are not compute accelerators.
    _INFRA_DESCRIPTIONS = {
        "iommu", "system peripheral", "generic system peripheral",
        "non-essential instrumentation", "encryption controller",
        "host bridge", "pci bridge", "isa bridge", "smi bridge",
        "signal processing controller", "communication controller",
        "pic", "dma controller", "timer",
        "performance counters",     # Intel CPU uncore PMU counters
        "scsi enclosure",           # Dell storage enclosure managers (Fryer U.2 etc.)
    }

    def find_accelerators(self, obj):
        """Route PCI devices under coprocessor/generic/processing classes.

        Known GPU vendors (Habana/Intel Gaudi, AMD, NVIDIA) get routed to
        self.gpus — they are general-purpose GPUs regardless of PCI class.

        Everything else goes to self.accelerators (Pliops, FPGAs, QAT, etc.).
        Chipset infrastructure (IOMMU, system peripherals) is filtered out.
        """
        if "product" not in obj:
            return
        description = obj.get("description", "").lower()
        # Skip chipset infrastructure
        if any(infra in description for infra in self._INFRA_DESCRIPTIONS):
            return

        vendor = obj.get("vendor", "Unknown")
        vendor_lower = vendor.lower()

        # Known GPU vendors under non-display PCI classes → route to GPUs
        if any(gv in vendor_lower for gv in self._GPU_VENDORS):
            self.find_gpus(obj)
            return

        # Everything else is a true accelerator (Pliops, FPGA, custom hardware)
        self.accelerators.append({
            "product": obj.get("product", "Unknown Accelerator"),
            "vendor": vendor,
            "description": obj.get("description", ""),
            "businfo": obj.get("businfo", ""),
            "class": obj.get("class", ""),
        })

    # PCI device classes that indicate compute accelerators (not CPUs, not GPUs)
    _ACCELERATOR_CLASSES = {"coprocessor", "generic", "processing"}

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
            elif cls in self._ACCELERATOR_CLASSES:
                self.find_accelerators(child)
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
                    elif gc_cls in self._ACCELERATOR_CLASSES:
                        self.find_accelerators(grandchild)
                    elif gc_cls == "bridge":
                        self.walk_bridge(grandchild)


if __name__ == "__main__":
    pass

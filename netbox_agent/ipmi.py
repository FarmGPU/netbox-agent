import logging
import subprocess

from netaddr import IPNetwork


class IPMI:
    """
    Parse IPMI output
    ie:

    Set in Progress         : Set Complete
    Auth Type Support       :
    Auth Type Enable        : Callback :
                            : User     :
                            : Operator :
                            : Admin    :
                            : OEM      :
    IP Address Source       : DHCP Address
    IP Address              : 10.192.2.1
    Subnet Mask             : 255.255.240.0
    MAC Address             : 98:f2:b3:f0:ee:1e
    SNMP Community String   :
    BMC ARP Control         : ARP Responses Enabled, Gratuitous ARP Disabled
    Default Gateway IP      : 10.192.2.254
    802.1q VLAN ID          : Disabled
    802.1q VLAN Priority    : 0
    RMCP+ Cipher Suites     : 0,1,2,3
    Cipher Suite Priv Max   : XuuaXXXXXXXXXXX
                            :     X=Cipher Suite Unused
                            :     c=CALLBACK
                            :     u=USER
                            :     o=OPERATOR
                            :     a=ADMIN
                            :     O=OEM
    Bad Password Threshold  : Not Available
    """

    def __init__(self):
        self.ret, self.output = subprocess.getstatusoutput("ipmitool lan print")
        if self.ret != 0:
            logging.warning("IPMI command failed: {}".format(self.output))

    def parse(self):
        _ipmi = {}

        for line in self.output.splitlines():
            key = line.split(":")[0].strip()
            if key not in ["802.1q VLAN ID", "IP Address", "Subnet Mask", "MAC Address"]:
                continue
            value = ":".join(line.split(":")[1:]).strip()
            _ipmi[key] = value

        ret = {}
        ret["name"] = "IPMI"
        ret["mtu"] = 1500
        ret["bonding"] = False
        try:
            ret["mac"] = _ipmi.get("MAC Address")
            if ret["mac"]:
                ret["mac"] = ret["mac"].upper()
            # VLAN ID is optional - some BMCs (like Supermicro) don't report it
            vlan_id = _ipmi.get("802.1q VLAN ID")
            if vlan_id and vlan_id != "Disabled":
                ret["vlan"] = int(vlan_id)
            else:
                ret["vlan"] = None
            ip = _ipmi.get("IP Address")
            netmask = _ipmi.get("Subnet Mask")
            if not ip or not netmask or not ret["mac"]:
                logging.error("IPMI decoding failed: missing required fields (IP=%s, Netmask=%s, MAC=%s)", 
                              ip, netmask, ret["mac"])
                return {}
        except (KeyError, ValueError) as e:
            logging.error("IPMI decoding failed: %s", e)
            return {}
        address = str(IPNetwork("{}/{}".format(ip, netmask)))

        ret["ip"] = [address]
        ret["ipmi"] = True
        return ret

FROM python:3.12-slim

# System packages for hardware discovery
RUN apt-get update && apt-get install -y --no-install-recommends \
    dmidecode \
    lshw \
    ethtool \
    ipmitool \
    smartmontools \
    pciutils \
    usbutils \
    kmod \
    net-tools \
    iproute2 \
    nvme-cli \
    && rm -rf /var/lib/apt/lists/*

# Install netbox-agent
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -e .

# Config is mounted at runtime
ENTRYPOINT ["netbox_agent"]
CMD ["-c", "/etc/netbox-agent/config.yaml", "-u"]

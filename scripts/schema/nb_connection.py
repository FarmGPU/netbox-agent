"""
Shared NetBox connection helper for schema setup scripts.
Reads NETBOX_URL and NETBOX_TOKEN from environment variables.
"""

import os
import sys
import logging
import pynetbox
import requests
import urllib3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def get_api():
    """Return a configured pynetbox Api instance."""
    url = os.environ.get("NETBOX_URL")
    token = os.environ.get("NETBOX_TOKEN")
    ssl_verify = os.environ.get("NETBOX_SSL_VERIFY", "true").lower() != "false"

    if not url or not token:
        logger.error("NETBOX_URL and NETBOX_TOKEN environment variables are required")
        sys.exit(1)

    nb = pynetbox.api(url=url, token=token)

    if not ssl_verify:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        session = requests.Session()
        session.verify = False
        nb.http_session = session

    logger.info("Connected to NetBox at %s (version %s)", url, nb.version)
    return nb

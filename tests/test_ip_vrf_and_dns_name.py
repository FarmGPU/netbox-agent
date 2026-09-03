"""Tests for VRF placement (DEV-91) and dns_name ownership (DEV-94).

Both bugs lived in create_or_update_netbox_ip_on_interface():

  DEV-91 — no `vrf` was ever set on create, so every IP the agent wrote
           landed in the global table even when its prefix belonged to a
           VRF. At lax01 that was 48 records plus one true duplicate.

  DEV-94 — dns_name was overwritten from the node's OS hostname on every
           run, so a tenant renaming their box destroyed the operator-set
           name in IPAM. Confirmed from the NetBox changelog: an operator
           repaired four rows at 17:50, the agent reverted them by 20:14.

These exercise the real helpers rather than a copy of them.
"""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Pre-mock netbox_agent.config — importing it for real parses sys.argv at
# import time, which pytest's own flags break. Same approach as
# tests/test_network_ip.py.
# ---------------------------------------------------------------------------
_mock_nb = MagicMock(name="nb")
_mock_config = SimpleNamespace(
    update_all=True, update_network=True, register=False,
    network=SimpleNamespace(
        ignore_interfaces="(dummy.*|docker.*)", ignore_ips="^(127\\.0\\.0\\..*)",
        ipmi=False, lldp=None, nic_id="name", primary_mac="temp",
    ),
)
_mock_config_module = MagicMock()
_mock_config_module.config = _mock_config
_mock_config_module.netbox_instance = _mock_nb
sys.modules.setdefault("netbox_agent.config", _mock_config_module)

_mock_misc = MagicMock()
_mock_misc.is_tool = MagicMock(return_value=False)
sys.modules.setdefault("netbox_agent.misc", _mock_misc)
sys.modules.setdefault("netbox_agent.ethtool", MagicMock())
sys.modules.setdefault("netbox_agent.lldp", MagicMock())
sys.modules.setdefault("netbox_agent.ipmi", MagicMock())

from netbox_agent import network as net  # noqa: E402

# Rebound per test by the autouse fixture below.
nbmock = None


@pytest.fixture(autouse=True)
def _isolate_module_state(monkeypatch):
    """Give each test a fresh NetBox mock and empty per-run caches.

    network.py binds `nb` at import time. Under the full suite another module
    may already have imported the real netbox_agent.config, in which case that
    `nb` is a live pynetbox client — so patch the attribute rather than trying
    to win the sys.modules race.
    """
    global nbmock
    nbmock = MagicMock(name="nb")
    monkeypatch.setattr(net, "nb", nbmock)
    monkeypatch.setattr(net, "_prefix_cache", None)
    monkeypatch.setattr(net, "_dns_cf_present", None)
    yield
    nbmock = None


def _prefix(cidr, vrf_id=None, vrf_name=None, status="active"):
    p = MagicMock(name=f"prefix-{cidr}")
    p.prefix = cidr
    p.status = SimpleNamespace(value=status)
    p.vrf = SimpleNamespace(id=vrf_id, name=vrf_name) if vrf_id else None
    return p


def _ip_record(rec_id, address, vrf_id=None, dns_name="", custom_fields=None):
    ip = MagicMock(name=f"ip-{rec_id}")
    ip.id = rec_id
    ip.address = address
    ip.vrf = SimpleNamespace(id=vrf_id, name=f"vrf{vrf_id}") if vrf_id else None
    ip.dns_name = dns_name
    ip.custom_fields = custom_fields if custom_fields is not None else {}
    return ip


def _net_obj(hostname="ash119"):
    """A Network instance with just enough wired up to call the helpers."""
    obj = net.Network.__new__(net.Network)
    obj.server = SimpleNamespace(get_hostname=lambda: hostname)
    return obj


# ---------------------------------------------------------------------------
# DEV-91 — VRF resolution
# ---------------------------------------------------------------------------

class TestVrfForAddress:
    def test_longest_match_wins(self):
        nbmock.ipam.prefixes.all.return_value = [
            _prefix("10.0.0.0/8", vrf_id=1, vrf_name="broad"),
            _prefix("10.0.23.0/24", vrf_id=2, vrf_name="VrfVhai-bgp"),
        ]
        assert net.vrf_for_address("10.0.23.47/32") == 2

    def test_no_containing_prefix_is_global(self):
        nbmock.ipam.prefixes.all.return_value = [
            _prefix("192.168.0.0/16", vrf_id=1, vrf_name="other"),
        ]
        assert net.vrf_for_address("10.0.23.47/32") is None

    def test_container_prefixes_are_ignored(self):
        """A container groups other prefixes; it does not own addresses."""
        nbmock.ipam.prefixes.all.return_value = [
            _prefix("10.0.0.0/8", vrf_id=1, vrf_name="container", status="container"),
        ]
        assert net.vrf_for_address("10.0.23.47/32") is None

    def test_prefix_without_vrf_is_global(self):
        nbmock.ipam.prefixes.all.return_value = [_prefix("10.0.23.0/24")]
        assert net.vrf_for_address("10.0.23.47/32") is None

    def test_ambiguous_tie_stays_global_and_does_not_raise(self):
        """netbox-workers raises here; the agent must not abort a sync."""
        nbmock.ipam.prefixes.all.return_value = [
            _prefix("10.0.23.0/24", vrf_id=2, vrf_name="vrf-a"),
            _prefix("10.0.23.0/24", vrf_id=3, vrf_name="vrf-b"),
        ]
        assert net.vrf_for_address("10.0.23.47/32") is None

    def test_prefix_fetch_failure_degrades_to_global(self):
        nbmock.ipam.prefixes.all.side_effect = Exception("netbox down")
        assert net.vrf_for_address("10.0.23.47/32") is None
        nbmock.ipam.prefixes.all.side_effect = None

    def test_unparseable_address_is_global(self):
        nbmock.ipam.prefixes.all.return_value = []
        assert net.vrf_for_address("not-an-ip") is None

    def test_prefixes_are_fetched_once_per_run(self):
        nbmock.ipam.prefixes.all.reset_mock()
        nbmock.ipam.prefixes.all.return_value = [
            _prefix("10.0.23.0/24", vrf_id=2, vrf_name="VrfVhai-bgp"),
        ]
        for _ in range(5):
            net.vrf_for_address("10.0.23.47/32")
        assert nbmock.ipam.prefixes.all.call_count == 1


# ---------------------------------------------------------------------------
# DEV-91 — record selection must never manufacture a duplicate
# ---------------------------------------------------------------------------

class TestSelectIpForVrf:
    def test_prefers_the_record_already_in_the_target_vrf(self):
        in_vrf = _ip_record(179, "10.0.23.47/32", vrf_id=2)
        obj = _net_obj()
        assert obj._select_ip_for_vrf([in_vrf], 2, "10.0.23.47/32") is in_vrf

    def test_moves_a_global_record_into_the_vrf(self):
        """The 48 misplaced lax01 records heal on the next run."""
        record = _ip_record(491, "10.0.23.47/32", vrf_id=None)
        obj = _net_obj()
        chosen = obj._select_ip_for_vrf([record], 2, "10.0.23.47/32")
        assert chosen is record
        assert record.vrf == 2
        record.save.assert_called_once()

    def test_conflict_uses_vrf_record_and_leaves_global_untouched(self):
        """Two populated records need a human, not a heuristic."""
        global_rec = _ip_record(491, "10.0.23.47/32", vrf_id=None, dns_name="ash037-vip")
        vrf_rec = _ip_record(179, "10.0.23.47/32", vrf_id=2, dns_name="ash111-vip")
        obj = _net_obj()
        chosen = obj._select_ip_for_vrf([global_rec, vrf_rec], 2, "10.0.23.47/32")
        assert chosen is vrf_rec
        global_rec.save.assert_not_called()

    def test_global_address_keeps_first_record(self):
        rec = _ip_record(1, "192.168.1.5/24")
        obj = _net_obj()
        assert obj._select_ip_for_vrf([rec], None, "192.168.1.5/24") is rec
        rec.save.assert_not_called()

    def test_failed_move_does_not_raise(self):
        record = _ip_record(491, "10.0.23.47/32", vrf_id=None)
        record.save.side_effect = Exception("409 conflict")
        obj = _net_obj()
        assert obj._select_ip_for_vrf([record], 2, "10.0.23.47/32") is record


# ---------------------------------------------------------------------------
# DEV-94 — dns_name ownership
# ---------------------------------------------------------------------------

class TestMayWriteDnsName:
    def test_writes_when_unset(self):
        obj = _net_obj("ash119")
        assert obj._may_write_dns_name(_ip_record(1, "10.0.23.53/32", dns_name="")) is True

    def test_protects_an_operator_set_name(self):
        """The exact DEV-94 case: tenant renamed the box to 'shadecloud'."""
        obj = _net_obj("shadecloud")
        record = _ip_record(185, "10.0.23.53/32", dns_name="ash119-vip")
        assert obj._may_write_dns_name(record) is False

    def test_follows_hostname_change_on_a_name_the_agent_owns(self):
        obj = _net_obj("ginger05")
        record = _ip_record(
            1, "10.1.1.1/24", dns_name="ginger04",
            custom_fields={net.DNS_NAME_OWNER_CF: net.DNS_NAME_OWNER},
        )
        assert obj._may_write_dns_name(record) is True

    def test_respects_a_third_party_owner(self):
        obj = _net_obj("ginger05")
        record = _ip_record(
            1, "10.1.1.1/24", dns_name="something",
            custom_fields={net.DNS_NAME_OWNER_CF: "dhcp-sync"},
        )
        assert obj._may_write_dns_name(record) is False

    def test_adopts_a_legacy_name_identical_to_the_hostname(self):
        """Migration path: no custom field yet, but the name is clearly ours."""
        obj = _net_obj("ginger04")
        record = _ip_record(1, "10.1.1.1/24", dns_name="ginger04", custom_fields={})
        assert obj._may_write_dns_name(record) is True

    def test_create_path_always_allowed(self):
        assert _net_obj()._may_write_dns_name(None) is True


class TestStampDnsName:
    def test_stamps_owner_when_field_exists(self):
        nbmock.extras.custom_fields.filter.return_value = [MagicMock()]
        record = _ip_record(1, "10.1.1.1/24", dns_name="old")
        assert _net_obj("new")._stamp_dns_name(record, "new") is True
        assert record.dns_name == "new"
        assert record.custom_fields[net.DNS_NAME_OWNER_CF] == net.DNS_NAME_OWNER

    def test_skips_stamp_when_custom_field_absent(self):
        """Missing CF must not break the write — the fix still holds."""
        nbmock.extras.custom_fields.filter.return_value = []
        record = _ip_record(1, "10.1.1.1/24", dns_name="old")
        assert _net_obj("new")._stamp_dns_name(record, "new") is True
        assert record.dns_name == "new"
        assert net.DNS_NAME_OWNER_CF not in record.custom_fields

    def test_preserves_other_custom_fields(self):
        nbmock.extras.custom_fields.filter.return_value = [MagicMock()]
        record = _ip_record(1, "10.1.1.1/24", dns_name="old",
                            custom_fields={"managed_by": "netbox-agent"})
        _net_obj("new")._stamp_dns_name(record, "new")
        assert record.custom_fields["managed_by"] == "netbox-agent"

    def test_no_write_when_already_correct(self):
        nbmock.extras.custom_fields.filter.return_value = [MagicMock()]
        record = _ip_record(1, "10.1.1.1/24", dns_name="same")
        assert _net_obj("same")._stamp_dns_name(record, "same") is False


class TestRegressionScenario:
    def test_lax01_rename_no_longer_propagates(self):
        """End-to-end of the reported bug, using the real changelog values."""
        nbmock.extras.custom_fields.filter.return_value = [MagicMock()]
        obj = _net_obj("shadecloud")           # tenant renamed the node
        record = _ip_record(185, "10.0.23.53/32", vrf_id=2, dns_name="ash119-vip")

        if obj._may_write_dns_name(record):
            obj._stamp_dns_name(record, obj._ip_dns_name())

        assert record.dns_name == "ash119-vip"

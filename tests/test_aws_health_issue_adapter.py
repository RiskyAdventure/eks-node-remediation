import importlib.util
import json
import os
from pathlib import Path

import pytest

ACCOUNT = "123456789012"
REGION = "us-west-2"
os.environ.update({
    "EXPECTED_ACCOUNT": ACCOUNT,
    "EXPECTED_REGION": REGION,
    "QUEUE_URL": "https://sqs.example/queue",
    "ALLOWED_SOURCES": "aws.health,custom.node-remediation.synthetic",
    "OBSERVE_ONLY": "false",
    "ENABLED_DRAIN_EVENT_CODES": "AWS_EC2_INSTANCE_STORE_DRIVE_PERFORMANCE_DEGRADED",
    "UNKNOWN_EVENT_CODE_ACTION": "ignore",
    "UNMAPPED_INSTANCE_POLICY": "skip",
    "MANAGED_NODE_LABEL_KEY": "workload",
    "MANAGED_NODE_LABEL_VALUES": "managed-gpu,managed-cpu",
})
spec = importlib.util.spec_from_file_location(
    "aws_health_issue_adapter",
    Path(__file__).parents[1] / "src/adapters/aws_health_issue_adapter.py",
)
adapter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(adapter)

INSTANCE = "i-0123456789abcdef0"
OTHER = "i-0fedcba9876543210"
CODE = "AWS_EC2_INSTANCE_STORE_DRIVE_PERFORMANCE_DEGRADED"


def event(*instances, event_id="delivery-1", event_arn="arn:aws:health:us-west-2::event/EC2/X/X_1", code=CODE, **detail_overrides):
    instances = instances or (INSTANCE,)
    detail = {
        "eventArn": event_arn,
        "service": "EC2",
        "eventTypeCode": code,
        "eventTypeCategory": "issue",
        "eventScopeCode": "ACCOUNT_SPECIFIC",
        "eventRegion": REGION,
        "affectedAccount": ACCOUNT,
        "statusCode": "open",
        "affectedEntities": [{"entityValue": i} for i in instances],
        **detail_overrides,
    }
    return {
        "version": "0",
        "id": event_id,
        "detail-type": "AWS Health Event",
        "source": "aws.health",
        "account": ACCOUNT,
        "region": REGION,
        "resources": list(instances),
        "detail": detail,
    }


def node(instance=INSTANCE, workload="managed-gpu", name="gpu-node"):
    return {
        "metadata": {"name": name, "uid": f"uid-{name}", "labels": {"workload": workload}},
        "spec": {"providerID": f"aws:///us-west-2a/{instance}"},
    }


class RecordingKube:
    def __init__(self, nodes, existing=()):
        self._nodes = nodes
        self.created = []
        self.existing = set(existing)

    def nodes(self):
        return self._nodes

    def create_drain_request(self, _event, target, source_event_id, policy):
        name = adapter.drain_request_name(source_event_id, adapter.PROVIDER_ID.fullmatch(target["spec"]["providerID"]).group(1))
        if name in self.existing:
            return name, False
        self.created.append((name, target["metadata"]["name"], policy))
        return name, True


# ----- validation -----------------------------------------------------------------

def test_validates_bare_ids_and_arns_and_dedups():
    arn = f"arn:aws:ec2:{REGION}:{ACCOUNT}:instance/{INSTANCE}"
    value = event()
    value["resources"] = [arn]
    assert adapter.validate_event(value) == [INSTANCE]


@pytest.mark.parametrize("field,value,expected", [
    ("account", "999999999999", "account"),
    ("region", "eu-west-1", "region"),
    ("source", "aws.ec2", "source"),
])
def test_rejects_wrong_envelope(field, value, expected):
    bad = event()
    bad[field] = value
    with pytest.raises(ValueError, match=expected):
        adapter.validate_event(bad)


@pytest.mark.parametrize("field,value,expected", [
    ("eventTypeCategory", "scheduledChange", "category"),
    ("eventScopeCode", "PUBLIC", "scope"),
    ("statusCode", "closed", "status"),
    ("service", "EBS", "service"),
    ("eventRegion", "eu-west-1", "event_region"),
    ("affectedAccount", "999999999999", "affected_account"),
])
def test_rejects_wrong_detail(field, value, expected):
    bad = event()
    bad["detail"][field] = value
    with pytest.raises(ValueError, match=expected):
        adapter.validate_event(bad)


def test_rejects_blast_radius_overflow(monkeypatch):
    monkeypatch.setattr(adapter, "MAX_INSTANCES_PER_EVENT", 2)
    with pytest.raises(ValueError, match="blast-radius"):
        adapter.validate_event(event(INSTANCE, OTHER, "i-00000000000000001"))


def test_event_without_instances_is_ignored_not_retried():
    value = event()
    value["detail"]["affectedEntities"] = [{"entityValue": "vol-123"}]
    value["resources"] = []
    assert adapter.validate_event(value) == []
    kube = RecordingKube([node()])
    result = adapter.process_message({"Body": json.dumps(value)}, kube)
    assert result["ignored"] is True and kube.created == []


def test_classified_codes_match_internal_fleet_precedent():
    """Internal EKS/GPU fleets action every scheduledChange (via NTH) and exactly one
    issue code. Instance-down notices, bandwidth constraints, AZ/network health, and
    UltraServer maintenance are deliberately not classified here."""
    assert adapter.POLICIES == {"AWS_EC2_INSTANCE_STORE_DRIVE_PERFORMANCE_DEGRADED": adapter.FATAL_DRAIN}
    for code in (
        "AWS_EC2_INSTANCE_CONSTRAINED_BANDWIDTH_ISSUE",
        "AWS_EC2_INSTANCE_AVAILABILITY_ISSUE",
        "AWS_EC2_INSTANCE_AUTO_RECOVERY_FAILURE",
        "AWS_EC2_ULTRASERVER_MAINTENANCE_INITIATED",
        "AWS_EC2_OPERATIONAL_ISSUE",
        "AWS_EC2_VPC_NETWORK_HEALTH_INTRA_AZ_ISSUE",
    ):
        assert code not in adapter.POLICIES
        assert adapter.policy_for(code) is None


def test_non_issue_categories_are_rejected():
    for category in ("accountNotification", "scheduledChange", "investigation"):
        bad = event(eventTypeCategory=category)
        with pytest.raises(ValueError, match="category"):
            adapter.validate_event(bad)


# ----- scheduledChange pre-retirement drain (opt-in) ----------------------------------

RETIREMENT = "AWS_EC2_INSTANCE_RETIREMENT_SCHEDULED"


def scheduled(*instances, start="Thu, 27 Aug 2026 13:19:03 GMT", code=RETIREMENT, **overrides):
    return event(*instances, code=code, eventTypeCategory="scheduledChange", startTime=start, **overrides)


def test_scheduled_change_is_rejected_unless_opted_in():
    with pytest.raises(ValueError, match="category"):
        adapter.validate_event(scheduled())


def test_scheduled_change_accepted_with_open_or_upcoming_status(monkeypatch):
    monkeypatch.setattr(adapter, "SCHEDULED_CHANGE_ACTION", "drain")
    assert adapter.validate_event(scheduled()) == [INSTANCE]
    assert adapter.validate_event(scheduled(statusCode="upcoming")) == [INSTANCE]
    with pytest.raises(ValueError, match="status"):
        adapter.validate_event(scheduled(statusCode="closed"))


def test_parse_health_time_accepts_rfc1123_and_iso():
    rfc = adapter.parse_health_time("Thu, 27 Aug 2026 13:19:03 GMT")
    iso = adapter.parse_health_time("2026-08-27T13:19:03Z")
    assert rfc == iso and rfc.tzinfo is not None
    assert adapter.parse_health_time("not a date") is None
    assert adapter.parse_health_time(None) is None


def test_scheduled_change_deadline_stops_before_start_within_crd_bounds():
    now = adapter.parse_health_time("2026-08-27T12:00:00Z")
    # start in 45 min, margin 30 min -> 15 min of polite eviction
    policy = adapter.scheduled_change_policy({"startTime": "2026-08-27T12:45:00Z"}, now=now)
    assert policy == {"faultClass": "Recoverable", "action": "DRAIN", "deadlineSeconds": 900}
    # far away -> CRD maximum; already inside the margin -> CRD minimum, still attempted
    assert adapter.scheduled_change_policy({"startTime": "2026-09-27T12:00:00Z"}, now=now)["deadlineSeconds"] == 3600
    assert adapter.scheduled_change_policy({"startTime": "2026-08-27T12:10:00Z"}, now=now)["deadlineSeconds"] == 60
    assert adapter.scheduled_change_policy({}, now=now)["deadlineSeconds"] == 3600


def test_scheduled_change_drains_every_code_in_the_category(monkeypatch):
    monkeypatch.setattr(adapter, "SCHEDULED_CHANGE_ACTION", "drain")
    kube = RecordingKube([node(INSTANCE), node(OTHER, name="cpu-node", workload="managed-cpu")])
    for code in (RETIREMENT, "AWS_EC2_PERSISTENT_INSTANCE_RETIREMENT_SCHEDULED", "AWS_EC2_INSTANCE_REBOOT_MAINTENANCE_SCHEDULED"):
        kube.created.clear()
        result = adapter.process_message({"Body": json.dumps(scheduled(INSTANCE, OTHER, code=code, event_arn=f"arn:x/{code}"))}, kube)
        assert len(result["created"]) == 2 and not result["ignored"]
        assert {p["action"] for _, _, p in kube.created} == {"DRAIN"}
        assert {p["faultClass"] for _, _, p in kube.created} == {"Recoverable"}


def test_scheduled_change_does_not_widen_issue_policy(monkeypatch):
    monkeypatch.setattr(adapter, "SCHEDULED_CHANGE_ACTION", "drain")
    kube = RecordingKube([node()])
    result = adapter.process_message({"Body": json.dumps(event(code="AWS_EC2_SOMETHING_UNKNOWN"))}, kube)
    assert result["ignored"] and not kube.created


# ----- idempotency -------------------------------------------------------------------

def test_drain_request_name_is_stable_across_health_updates_and_pages():
    first = event(event_id="delivery-1", page="1", totalPages="2")
    update = event(event_id="delivery-2", page="2", totalPages="2")
    assert adapter.stable_event_id(first) == adapter.stable_event_id(update)
    name_a = adapter.drain_request_name(adapter.stable_event_id(first), INSTANCE)
    name_b = adapter.drain_request_name(adapter.stable_event_id(update), INSTANCE)
    assert name_a == name_b and name_a.startswith("aws-health-")
    assert adapter.drain_request_name("x", INSTANCE) != adapter.drain_request_name("x", OTHER)


def test_existing_request_is_not_recreated():
    kube = RecordingKube([node()])
    kube.existing.add(adapter.drain_request_name(adapter.stable_event_id(event()), INSTANCE))
    result = adapter.process_message({"Body": json.dumps(event())}, kube)
    assert kube.created == []
    assert len(result["created"]) == 1


# ----- mapping policy --------------------------------------------------------------

def test_maps_exact_provider_id_and_flags_ambiguity():
    mapped = adapter.map_nodes([INSTANCE], [node(OTHER, name="other"), node()])
    assert mapped[INSTANCE]["metadata"]["name"] == "gpu-node"
    assert adapter.map_nodes([OTHER], [node()])[OTHER] is None
    with pytest.raises(ValueError, match="ambiguous"):
        adapter.map_nodes([INSTANCE], [node(name="a"), node(name="b")])


def test_unmapped_and_unmanaged_instances_are_skipped_by_default():
    kube = RecordingKube([node(), node(OTHER, workload="scale-test", name="unmanaged")])
    result = adapter.process_message({"Body": json.dumps(event(INSTANCE, OTHER, "i-00000000000000001"))}, kube)
    assert [c[1] for c in kube.created] == ["gpu-node"]
    assert set(result["skipped"]) == {OTHER, "i-00000000000000001"}


def test_unmapped_policy_fail_blocks_whole_message(monkeypatch):
    monkeypatch.setattr(adapter, "UNMAPPED_INSTANCE_POLICY", "fail")
    kube = RecordingKube([node()])
    with pytest.raises(ValueError, match="UNMAPPED_INSTANCE_POLICY=fail"):
        adapter.process_message({"Body": json.dumps(event(INSTANCE, OTHER))}, kube)
    assert kube.created == []


def test_ambiguous_mapping_always_fails():
    kube = RecordingKube([node(name="a"), node(name="b")])
    with pytest.raises(ValueError, match="ambiguous"):
        adapter.process_message({"Body": json.dumps(event())}, kube)
    assert kube.created == []


# ----- event-code policy ---------------------------------------------------------------

def test_enabled_code_maps_to_fatal_drain():
    kube = RecordingKube([node()])
    adapter.process_message({"Body": json.dumps(event())}, kube)
    _, _, policy = kube.created[0]
    assert (policy["faultClass"], policy["action"]) == ("Fatal", "DRAIN")


def test_unknown_code_is_ignored_by_default():
    kube = RecordingKube([node()])
    result = adapter.process_message({"Body": json.dumps(event(code="AWS_EC2_OPERATIONAL_ISSUE"))}, kube)
    assert kube.created == []
    assert result["ignored"] is True


def test_unknown_code_can_preserve_when_opted_in(monkeypatch):
    monkeypatch.setattr(adapter, "UNKNOWN_EVENT_CODE_ACTION", "preserve")
    kube = RecordingKube([node()])
    adapter.process_message({"Body": json.dumps(event(code="AWS_EC2_OPERATIONAL_ISSUE"))}, kube)
    assert kube.created[0][2]["action"] == "PRESERVE"


def test_enabled_but_unclassified_code_preserves(monkeypatch):
    monkeypatch.setattr(adapter, "ENABLED_DRAIN_EVENT_CODES", {"AWS_EC2_SOMETHING_NEW"})
    assert adapter.policy_for("AWS_EC2_SOMETHING_NEW") == adapter.PRESERVE_POLICY
    assert adapter.policy_for(CODE) is None


def test_observe_only_maps_without_creating(monkeypatch):
    monkeypatch.setattr(adapter, "OBSERVE_ONLY", True)
    kube = RecordingKube([node()])
    result = adapter.process_message({"Body": json.dumps(event())}, kube)
    assert kube.created == [] and result["created"] == []


# ----- DrainRequest body -----------------------------------------------------------------

class FakeKube(adapter.KubeClient):
    def __init__(self, get_status=404, post_status=None):
        self.calls = []
        self.get_status = get_status
        self.post_status = post_status

    def request(self, method, path, body=None):
        self.calls.append((method, path, body))
        if method == "GET":
            if self.get_status == 200:
                return {"metadata": {"name": "exists"}}
            raise adapter.ApiError(self.get_status, "not found")
        if self.post_status:
            raise adapter.ApiError(self.post_status, "conflict")
        return body


def test_creates_contract_compatible_drain_request():
    kube = FakeKube()
    name, created = kube.create_drain_request(event(), node(), "health-arn", adapter.POLICIES[CODE])
    assert created is True
    body = kube.calls[-1][2]
    assert body["apiVersion"] == "nvsentinel.nvidia.com/v1alpha1"
    assert body["kind"] == "DrainRequest"
    assert body["metadata"]["namespace"] == "node-remediation-system"
    assert body["spec"] == {
        "nodeName": "gpu-node",
        "nodeUID": "uid-gpu-node",
        "sourceProvider": "aws",
        "sourceEventID": "health-arn",
        "faultClass": "Fatal",
        "faultCode": CODE,
        "action": "DRAIN",
        "approved": True,
        "deadlineSeconds": 300,
        "allowForceAfterDeadline": False,
        "groupPolicy": "NodeLocal",
    }
    assert body["metadata"]["annotations"]["nvsentinel.nvidia.com/aws-instance-id"] == INSTANCE


def test_conflict_on_create_is_treated_as_existing():
    kube = FakeKube(post_status=409)
    _, created = kube.create_drain_request(event(), node(), "health-arn", adapter.POLICIES[CODE])
    assert created is False


def test_get_hit_short_circuits_create():
    kube = FakeKube(get_status=200)
    _, created = kube.create_drain_request(event(), node(), "health-arn", adapter.POLICIES[CODE])
    assert created is False
    assert [c[0] for c in kube.calls] == ["GET"]

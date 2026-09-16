# SPDX-License-Identifier: Apache-2.0
"""AWS Health EC2 adapter (issue events, optionally scheduledChange events).

Consumes AWS Health EventBridge events from SQS, validates the envelope, maps
affected EC2 instances to managed Kubernetes Nodes by providerID, and creates
DrainRequest objects for the remediation controller. It never mutates pods or
nodes and never calls EC2/Auto Scaling APIs.
"""
import datetime as dt
import email.utils
import hashlib
import json
import os
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request

import boto3

API_GROUP = "nvsentinel.nvidia.com"
API_VERSION = "v1alpha1"
RESOURCE = "drainrequests"
COMPONENT = "aws-health-issue-adapter"
USER_AGENT = f"{COMPONENT}/0.3.0"
INSTANCE_ID = re.compile(r"^i-[0-9a-f]{8,17}$")
PROVIDER_ID = re.compile(r"^aws:///[a-z0-9-]+/(i-[0-9a-f]{8,17})$")
EXPECTED_ACCOUNT = os.environ["EXPECTED_ACCOUNT"]
EXPECTED_REGION = os.environ["EXPECTED_REGION"]
QUEUE_URL = os.environ["QUEUE_URL"]
DRAIN_NAMESPACE = os.environ.get("DRAIN_NAMESPACE", "node-remediation-system")
ALLOWED_SOURCES = {value for value in os.environ.get("ALLOWED_SOURCES", "aws.health").split(",") if value}
OBSERVE_ONLY = os.environ.get("OBSERVE_ONLY", "true").lower() == "true"
ENABLED_DRAIN_EVENT_CODES = {
    value for value in os.environ.get("ENABLED_DRAIN_EVENT_CODES", "").split(",") if value
}
# ignore: create nothing for event codes that are not explicitly enabled.
# preserve: cordon-only DrainRequest (PRESERVE) for codes that are not enabled.
UNKNOWN_EVENT_CODE_ACTION = os.environ.get("UNKNOWN_EVENT_CODE_ACTION", "ignore").lower()
# skip: instances with no managed Node in this cluster are logged and acknowledged.
# fail: any unmapped instance fails the whole message (retries, then dead-letter queue).
UNMAPPED_INSTANCE_POLICY = os.environ.get("UNMAPPED_INSTANCE_POLICY", "skip").lower()
MAX_INSTANCES_PER_EVENT = int(os.environ.get("MAX_INSTANCES_PER_EVENT", "25"))
MANAGED_NODE_LABEL_KEY = os.environ.get("MANAGED_NODE_LABEL_KEY", "workload")
MANAGED_NODE_LABEL_VALUES = {
    value.strip()
    for value in (
        os.environ.get("MANAGED_NODE_LABEL_VALUES") or os.environ.get("MANAGED_NODE_WORKLOADS", "")
    ).split(",")
    if value.strip()
}

# ignore: scheduledChange events are not consumed here (NTH cordons them by category).
# drain: also turn every EC2 scheduledChange (retirement, host reboot/maintenance) into
#        a budgeted DRAIN so the node is emptied under the controller's node-group
#        budget well before EC2 acts, instead of every affected node hard-stopping at
#        the same scheduled minute. Requires the ScheduledChangeRuleState rule.
SCHEDULED_CHANGE_ACTION = os.environ.get("SCHEDULED_CHANGE_ACTION", "ignore").lower()
# The drain deadline is set so eviction attempts stop this many seconds before the
# scheduled start; clamped to the CRD's deadlineSeconds range.
SCHEDULED_CHANGE_MARGIN_SECONDS = int(os.environ.get("SCHEDULED_CHANGE_MARGIN_SECONDS", "1800"))
DEADLINE_MIN, DEADLINE_MAX = 60, 3600

FATAL_DRAIN = {"faultClass": "Fatal", "action": "DRAIN", "deadlineSeconds": 300}
PRESERVE_POLICY = {"faultClass": "Unknown", "action": "PRESERVE", "deadlineSeconds": 900}
SCHEDULED_DRAIN = {"faultClass": "Recoverable", "action": "DRAIN", "deadlineSeconds": DEADLINE_MAX}

# Per-instance AWS Health EC2 *issue* codes and the policy applied when the code is
# listed in ENABLED_DRAIN_EVENT_CODES. This is deliberately one code. Every other
# per-instance Health signal is either a scheduledChange (handled by NTH by category)
# or an "instance is already down" notice that EC2 auto-recovery and the ASG health
# check already act on, where draining achieves nothing. See docs/ARCHITECTURE.md
# "Which AWS Health signals are acted on".
POLICIES = {
    # Local instance-store (NVMe) drive is degrading; running work on it is at risk.
    "AWS_EC2_INSTANCE_STORE_DRIVE_PERFORMANCE_DEGRADED": FATAL_DRAIN,
}

if UNKNOWN_EVENT_CODE_ACTION not in {"ignore", "preserve"}:
    raise SystemExit("UNKNOWN_EVENT_CODE_ACTION must be ignore or preserve")
if SCHEDULED_CHANGE_ACTION not in {"ignore", "drain"}:
    raise SystemExit("SCHEDULED_CHANGE_ACTION must be ignore or drain")
if UNMAPPED_INSTANCE_POLICY not in {"skip", "fail"}:
    raise SystemExit("UNMAPPED_INSTANCE_POLICY must be skip or fail")
if not MANAGED_NODE_LABEL_VALUES:
    raise SystemExit("MANAGED_NODE_LABEL_VALUES must contain at least one Node label value")


def policy_for(event_type):
    """Return the DrainRequest policy for an issue event code, or None for no action."""
    if event_type in ENABLED_DRAIN_EVENT_CODES:
        return POLICIES.get(event_type, PRESERVE_POLICY)
    if UNKNOWN_EVENT_CODE_ACTION == "preserve":
        return PRESERVE_POLICY
    return None


def parse_health_time(value):
    """AWS Health EventBridge timestamps are RFC 1123 ('Thu, 27 Aug 2026 13:19:03 GMT');
    accept ISO 8601 too. Return an aware UTC datetime or None."""
    if not value:
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(str(value))
    except (TypeError, ValueError):
        try:
            parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def scheduled_change_policy(detail, now=None):
    """DRAIN policy for a scheduledChange: keep evicting politely until
    SCHEDULED_CHANGE_MARGIN_SECONDS before the scheduled start, within the CRD's
    deadline range. Unknown or distant start times use the maximum deadline; a start
    that is already inside the margin uses the minimum so the attempt still happens."""
    now = now or dt.datetime.now(dt.timezone.utc)
    start = parse_health_time(detail.get("startTime"))
    if start is None:
        return dict(SCHEDULED_DRAIN)
    remaining = int((start - now).total_seconds()) - SCHEDULED_CHANGE_MARGIN_SECONDS
    return {**SCHEDULED_DRAIN, "deadlineSeconds": max(DEADLINE_MIN, min(DEADLINE_MAX, remaining))}


def log(outcome, **fields):
    print(json.dumps({"outcome": outcome, **fields}, sort_keys=True), flush=True)


class ApiError(RuntimeError):
    def __init__(self, status, body):
        super().__init__(f"Kubernetes API returned {status}: {body}")
        self.status = status
        self.body = body


def instance_id(value):
    candidate = str(value).rsplit("/", 1)[-1]
    return candidate if INSTANCE_ID.fullmatch(candidate) else None


def accepted_categories():
    return {"issue", "scheduledChange"} if SCHEDULED_CHANGE_ACTION == "drain" else {"issue"}


def validate_event(event):
    detail = event.get("detail", {})
    checks = {
        "account": event.get("account") == EXPECTED_ACCOUNT,
        "region": event.get("region") == EXPECTED_REGION,
        "source": event.get("source") in ALLOWED_SOURCES,
        "detail_type": event.get("detail-type") == "AWS Health Event",
        "service": detail.get("service") == "EC2",
        "category": detail.get("eventTypeCategory") in accepted_categories(),
        "scope": detail.get("eventScopeCode") == "ACCOUNT_SPECIFIC",
        "status": detail.get("statusCode") in {"open", "upcoming"},
        # Present on real AWS Health events; must agree with the envelope when present.
        "affected_account": detail.get("affectedAccount", EXPECTED_ACCOUNT) == EXPECTED_ACCOUNT,
        "event_region": detail.get("eventRegion", EXPECTED_REGION) == EXPECTED_REGION,
    }
    failed = [name for name, valid in checks.items() if not valid]
    if failed:
        raise ValueError(f"event failed validation: {','.join(failed)}")

    values = [entity.get("entityValue", "") for entity in detail.get("affectedEntities", [])]
    values.extend(event.get("resources", []))
    ids = sorted({value for raw in values if (value := instance_id(raw))})
    if len(ids) > MAX_INSTANCES_PER_EVENT:
        raise ValueError(f"event exceeds the {MAX_INSTANCES_PER_EVENT}-node blast-radius limit")
    return ids


def stable_event_id(event):
    """Stable across AWS Health updates and affectedEntities pagination: prefer the
    Health event ARN over the per-delivery EventBridge id."""
    return event.get("detail", {}).get("eventArn") or event.get("id") or "unknown"


def node_is_managed(node):
    labels = node.get("metadata", {}).get("labels", {})
    return labels.get(MANAGED_NODE_LABEL_KEY) in MANAGED_NODE_LABEL_VALUES


def map_nodes(instance_ids, nodes):
    """Return {instanceId: Node | None}. Raise on ambiguous providerID matches."""
    mapped = {}
    for target in instance_ids:
        matches = []
        for node in nodes:
            provider = node.get("spec", {}).get("providerID", "")
            match = PROVIDER_ID.fullmatch(provider)
            if match and match.group(1) == target:
                matches.append(node)
        if len(matches) > 1:
            raise ValueError(f"ambiguous Kubernetes Node mapping for {target}")
        mapped[target] = matches[0] if matches else None
    return mapped


def drain_request_name(source_event_id, instance):
    digest = hashlib.sha256(f"{source_event_id}:{instance}".encode()).hexdigest()[:20]
    return f"aws-health-{digest}"


class KubeClient:
    TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

    def __init__(self):
        host = os.environ["KUBERNETES_SERVICE_HOST"]
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        self.base = f"https://{host}:{port}"
        self.context = ssl.create_default_context(cafile=self.CA_PATH)

    def token(self):
        with open(self.TOKEN_PATH, encoding="utf-8") as token_file:
            return token_file.read().strip()

    def request(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            f"{self.base}{path}",
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token()}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": USER_AGENT,
            },
        )
        try:
            with urllib.request.urlopen(request, context=self.context, timeout=15) as response:
                return json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            body_text = error.read().decode(errors="replace")
            raise ApiError(error.code, body_text) from error

    def nodes(self):
        return self.request("GET", "/api/v1/nodes").get("items", [])

    def create_drain_request(self, event, node, source_event_id, policy):
        detail = event["detail"]
        event_type = detail.get("eventTypeCode", "UNKNOWN")
        instance = PROVIDER_ID.fullmatch(node["spec"]["providerID"]).group(1)
        name = drain_request_name(source_event_id, instance)
        namespace = urllib.parse.quote(DRAIN_NAMESPACE, safe="")
        path = f"/apis/{API_GROUP}/{API_VERSION}/namespaces/{namespace}/{RESOURCE}/{name}"
        try:
            self.request("GET", path)
            return name, False
        except ApiError as error:
            if error.status != 404:
                raise

        body = {
            "apiVersion": f"{API_GROUP}/{API_VERSION}",
            "kind": "DrainRequest",
            "metadata": {
                "name": name,
                "namespace": DRAIN_NAMESPACE,
                "labels": {
                    "app.kubernetes.io/managed-by": COMPONENT,
                    f"{API_GROUP}/source-provider": "aws",
                },
                "annotations": {
                    f"{API_GROUP}/aws-health-event-arn": detail.get("eventArn", ""),
                    f"{API_GROUP}/aws-instance-id": instance,
                },
            },
            "spec": {
                "nodeName": node["metadata"]["name"],
                "nodeUID": node["metadata"]["uid"],
                "sourceProvider": "aws",
                "sourceEventID": source_event_id[:256],
                "faultClass": policy["faultClass"],
                "faultCode": event_type[:256],
                "action": policy["action"],
                "approved": True,
                "deadlineSeconds": policy["deadlineSeconds"],
                "allowForceAfterDeadline": False,
                "groupPolicy": "NodeLocal",
            },
        }
        collection = f"/apis/{API_GROUP}/{API_VERSION}/namespaces/{namespace}/{RESOURCE}"
        try:
            self.request("POST", collection, body)
        except ApiError as error:
            if error.status == 409:
                return name, False
            raise
        return name, True


def process_message(message, kube):
    event = json.loads(message["Body"])
    instance_ids = validate_event(event)
    source_event_id = stable_event_id(event)
    event_type = event["detail"].get("eventTypeCode", "UNKNOWN")
    if not instance_ids:
        # Account-specific EC2 issue with no instance entities (marketplace, reserved
        # instance, or fleet-level notices). Nothing to map; acknowledge, do not retry.
        log("event_ignored", event_id=source_event_id, event_type=event_type, reason="no EC2 instance IDs in event")
        return {"created": [], "skipped": [], "ignored": True}
    if event["detail"].get("eventTypeCategory") == "scheduledChange":
        # Category-level policy, like NTH: every EC2 scheduled change means the host
        # is going away at startTime. validate_event only admits the category when
        # SCHEDULED_CHANGE_ACTION=drain.
        policy = scheduled_change_policy(event["detail"])
    else:
        policy = policy_for(event_type)
    if policy is None:
        log(
            "event_ignored",
            event_id=source_event_id,
            event_type=event_type,
            instance_ids=instance_ids,
            reason="event code not enabled and UNKNOWN_EVENT_CODE_ACTION=ignore",
        )
        return {"created": [], "skipped": instance_ids, "ignored": True}

    mappings = map_nodes(instance_ids, kube.nodes())
    missing, unmanaged, managed = [], [], {}
    for instance, node in mappings.items():
        if node is None:
            missing.append(instance)
        elif not node_is_managed(node):
            unmanaged.append(instance)
            log("node_not_managed", instance_id=instance, node=node["metadata"]["name"], event_id=source_event_id)
        else:
            managed[instance] = node
    for instance in missing:
        log("node_not_found", instance_id=instance, event_id=source_event_id)
    if (missing or unmanaged) and UNMAPPED_INSTANCE_POLICY == "fail":
        raise ValueError(
            "unmapped or unmanaged instance(s) with UNMAPPED_INSTANCE_POLICY=fail: "
            + ",".join(missing + unmanaged)
        )

    created = []
    for instance, node in managed.items():
        if OBSERVE_ONLY:
            log(
                "event_observed",
                event_id=source_event_id,
                event_type=event_type,
                instance_id=instance,
                node=node["metadata"]["name"],
                would_create=drain_request_name(source_event_id, instance),
                policy=policy,
            )
            continue
        name, was_created = kube.create_drain_request(event, node, source_event_id, policy)
        created.append(name)
        log(
            "drain_request_created" if was_created else "drain_request_exists",
            drain_request=name,
            instance_id=instance,
            node=node["metadata"]["name"],
            event_id=source_event_id,
            event_type=event_type,
        )
    return {"created": created, "skipped": missing + unmanaged, "ignored": False}


def main():
    sqs = boto3.client("sqs", region_name=EXPECTED_REGION)
    kube = KubeClient()
    log(
        "started",
        queue_url=QUEUE_URL,
        drain_namespace=DRAIN_NAMESPACE,
        observe_only=OBSERVE_ONLY,
        enabled_drain_event_codes=sorted(ENABLED_DRAIN_EVENT_CODES),
        unknown_event_code_action=UNKNOWN_EVENT_CODE_ACTION,
        unmapped_instance_policy=UNMAPPED_INSTANCE_POLICY,
        managed_node_label_key=MANAGED_NODE_LABEL_KEY,
        managed_node_label_values=sorted(MANAGED_NODE_LABEL_VALUES),
    )
    while True:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
            VisibilityTimeout=60,
            AttributeNames=["ApproximateReceiveCount"],
        )
        for message in response.get("Messages", []):
            try:
                process_message(message, kube)
                sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])
            except Exception as error:
                log(
                    "message_error",
                    message_id=message.get("MessageId"),
                    receive_count=message.get("Attributes", {}).get("ApproximateReceiveCount"),
                    error=str(error),
                )
        if not response.get("Messages"):
            time.sleep(1)


if __name__ == "__main__":
    main()

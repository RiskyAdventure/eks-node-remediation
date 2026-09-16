# SPDX-License-Identifier: Apache-2.0
"""Drain-policy controller for DrainRequest custom resources.

Implements the NVSentinel custom-drain plugin contract (group
nvsentinel.nvidia.com/v1alpha1, kind DrainRequest, completion condition
DrainComplete=True) with a customer workload policy: UID-fenced cordon, durable
pod selection, PDB-aware Eviction, deadline handling, and separately gated purge.

The controller never deletes Nodes, never calls AWS APIs, and never changes
capacity. Node replacement belongs to the customer's capacity layer.
"""
import datetime as dt
import json
import os
import signal
import socket
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

API_GROUP = "nvsentinel.nvidia.com"
API_VERSION = "v1alpha1"
RESOURCE = "drainrequests"
KIND = "DrainRequest"
GPU_RESOURCE = "nvidia.com/gpu"
GROUP_ANNOTATION = f"{API_GROUP}/group-id"
COMPONENT = "node-remediation-controller"
USER_AGENT = f"{COMPONENT}/0.3.0"
DEFAULT_ACTIONS = {
    "Unknown": "PRESERVE",
    "Recoverable": "DRAIN",
    "Fatal": "DRAIN",
    "Integrity": "PURGE",
}
TERMINAL_PHASES = {"Succeeded", "Failed"}
TERMINAL_REQUEST_PHASES = {"Completed", "NodeNotFound", "TimedOut", "PurgeNotAuthorized"}
# Phases in which a request is actively reducing a node group's capacity. These count
# against MAX_CONCURRENT_PER_NODE_GROUP. Preserving is excluded: the cordon is done and
# the request is waiting on humans; remaining capacity is guarded by the min-ready gate.
IN_FLIGHT_PHASES = {"NodeLocked", "TargetsLocked", "Evicting", "DrainBlocked", "Purging"}
DRAIN_COMPLETE = "DrainComplete"
DEFAULT_NODE_GROUP_LABEL_KEY = "eks.amazonaws.com/nodegroup"


class ApiError(RuntimeError):
    def __init__(self, status, body):
        super().__init__(f"Kubernetes API returned {status}: {body}")
        self.status = status
        self.body = body


def log(outcome, **fields):
    print(json.dumps({"outcome": outcome, **fields}, sort_keys=True), flush=True)


def utcnow():
    return dt.datetime.now(dt.timezone.utc)


def parse_time(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def format_time(value):
    return value.astimezone(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def split_csv(value):
    return {item.strip() for item in value.split(",") if item.strip()}


def managed_label_key():
    return os.environ.get("MANAGED_NODE_LABEL_KEY", "workload")


def managed_label_values():
    raw = os.environ.get("MANAGED_NODE_LABEL_VALUES") or os.environ.get("MANAGED_NODE_WORKLOADS", "")
    return split_csv(raw)


def effective_action(spec):
    return spec.get("action") or DEFAULT_ACTIONS[spec["faultClass"]]


def resource_requests_gpu(container):
    resources = container.get("resources", {})
    for section in ("requests", "limits"):
        value = resources.get(section, {}).get(GPU_RESOURCE)
        if value not in (None, 0, "0"):
            return True
    return False


def is_gpu_pod(pod):
    spec = pod.get("spec", {})
    containers = spec.get("containers", []) + spec.get("initContainers", [])
    return any(resource_requests_gpu(container) for container in containers)


def is_actionable_pod(pod):
    metadata = pod.get("metadata", {})
    if pod.get("status", {}).get("phase") in TERMINAL_PHASES:
        return False
    if "kubernetes.io/config.mirror" in metadata.get("annotations", {}):
        return False
    owners = metadata.get("ownerReferences", [])
    return not any(owner.get("kind") == "DaemonSet" for owner in owners)


def node_is_managed(node, label_key, label_values):
    labels = node.get("metadata", {}).get("labels", {})
    return label_key in labels and labels[label_key] in label_values


def purge_authorized(spec, enable_purge):
    return all((
        enable_purge,
        spec.get("approved") is True,
        spec.get("allowForceAfterDeadline") is True,
        effective_action(spec) == "PURGE",
        spec.get("faultClass") in {"Fatal", "Integrity"},
    ))


def allowed_namespace(namespace, allowed):
    return not allowed or namespace in allowed


def select_targets(node_pods, all_pods, group_policy, allowed):
    local_workloads = [
        pod for pod in node_pods
        if is_actionable_pod(pod)
        and allowed_namespace(pod["metadata"]["namespace"], allowed)
    ]
    if group_policy == "NodeLocal":
        return local_workloads
    if not local_workloads:
        return []

    group_ids = {
        pod.get("metadata", {}).get("annotations", {}).get(GROUP_ANNOTATION)
        for pod in local_workloads
    }
    if None in group_ids or "" in group_ids or len(group_ids) != 1:
        raise ValueError("Gang remediation requires one explicit group-id on every local workload pod")
    group_id = next(iter(group_ids))
    targets = [
        pod for pod in all_pods
        if is_actionable_pod(pod)
        and allowed_namespace(pod["metadata"]["namespace"], allowed)
        and pod.get("metadata", {}).get("annotations", {}).get(GROUP_ANNOTATION) == group_id
    ]
    if not targets:
        raise ValueError(f"Gang group {group_id} resolved to no actionable pods")
    return targets


def pod_ref(pod):
    metadata = pod["metadata"]
    return {
        "namespace": metadata["namespace"],
        "name": metadata["name"],
        "uid": metadata["uid"],
        "gpu": is_gpu_pod(pod),
    }


def json_pointer(value):
    return value.replace("~", "~0").replace("/", "~1")


def request_sort_key(remediation):
    metadata = remediation.get("metadata", {})
    return (metadata.get("creationTimestamp") or "", metadata.get("name", ""))


def is_terminal(remediation):
    status = remediation.get("status", {})
    return (
        status.get("phase") in TERMINAL_REQUEST_PHASES
        and status.get("observedGeneration") == remediation.get("metadata", {}).get("generation")
    )


def node_owners(remediations):
    """Return {nodeName: owning DrainRequest} choosing the oldest non-terminal request."""
    owners = {}
    for remediation in sorted(remediations, key=request_sort_key):
        if is_terminal(remediation):
            continue
        node_name = remediation.get("spec", {}).get("nodeName")
        if node_name and node_name not in owners:
            owners[node_name] = remediation
    return owners


def is_admitted(remediation):
    """A request is admitted once the controller has persisted its Node fence; from
    then on the node-group gates no longer apply (a cordoned node must not be
    abandoned half-way because a sibling request appeared)."""
    return bool(remediation.get("status", {}).get("nodeUID"))


def is_in_flight(remediation):
    return not is_terminal(remediation) and remediation.get("status", {}).get("phase") in IN_FLIGHT_PHASES


def node_is_ready(node):
    for condition in node.get("status", {}).get("conditions", []):
        if condition.get("type") == "Ready":
            return condition.get("status") == "True"
    return False


def node_group_of(node, group_key, managed_key):
    """Node group identity: the EKS managed-node-group label when present, otherwise
    the managed label value so self-managed groups still get a budget."""
    labels = node.get("metadata", {}).get("labels", {})
    return labels.get(group_key) or labels.get(managed_key)


def node_group_gate(target_name, nodes_by_name, remediations, group_key, managed_key, max_in_flight, min_ready):
    """Return None when the request may be admitted, else (reason, message).

    nodes_by_name: every managed Node in the cluster (one list per reconcile pass).
    remediations: every DrainRequest (the caller's own request is excluded by name
    matching on the target node, which cannot be in flight yet if it is unadmitted).
    """
    target = nodes_by_name.get(target_name)
    if target is None:
        return None  # reconcile will report NodeNotFound or the allowlist error
    group = node_group_of(target, group_key, managed_key)
    if group is None:
        return None
    in_flight_nodes = set()
    for remediation in remediations:
        if not is_in_flight(remediation):
            continue
        other = nodes_by_name.get(remediation.get("spec", {}).get("nodeName"))
        if other is not None and node_group_of(other, group_key, managed_key) == group:
            in_flight_nodes.add(other["metadata"]["name"])
    in_flight_nodes.discard(target_name)
    if max_in_flight > 0 and len(in_flight_nodes) >= max_in_flight:
        return (
            "NodeGroupBudget",
            f"Node group {group} has {len(in_flight_nodes)} of {max_in_flight} remediation(s) in flight "
            f"({', '.join(sorted(in_flight_nodes))}); waiting",
        )
    if min_ready > 0:
        available = [
            name for name, candidate in nodes_by_name.items()
            if name != target_name
            and name not in in_flight_nodes
            and node_group_of(candidate, group_key, managed_key) == group
            and node_is_ready(candidate)
            and candidate.get("spec", {}).get("unschedulable") is not True
        ]
        if len(available) < min_ready:
            return (
                "NodeGroupCapacity",
                f"Node group {group} would keep only {len(available)} ready schedulable node(s), "
                f"below MIN_READY_NODES_PER_NODE_GROUP={min_ready}; waiting",
            )
    return None


class KubeClient:
    """Minimal in-cluster client. The projected token is re-read on every request so
    kubelet token rotation is honored without a restart."""

    TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

    def __init__(self):
        host = os.environ.get("KUBERNETES_SERVICE_HOST")
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        if not host:
            raise RuntimeError("KUBERNETES_SERVICE_HOST is required")
        self.base = f"https://{host}:{port}"
        self.context = ssl.create_default_context(cafile=self.CA_PATH)

    def token(self):
        with open(self.TOKEN_PATH, encoding="utf-8") as token_file:
            return token_file.read().strip()

    def request(self, method, path, body=None, content_type="application/json"):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            f"{self.base}{path}",
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token()}",
                "Accept": "application/json",
                "Content-Type": content_type,
                "User-Agent": USER_AGENT,
            },
        )
        try:
            with urllib.request.urlopen(request, context=self.context, timeout=15) as response:
                return json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            body_text = error.read().decode(errors="replace")
            raise ApiError(error.code, body_text) from error

    def get(self, path):
        return self.request("GET", path)

    def merge_patch(self, path, body):
        return self.request("PATCH", path, body, "application/merge-patch+json")

    def json_patch(self, path, body):
        return self.request("PATCH", path, body, "application/json-patch+json")


class LeaderLease:
    """coordination.k8s.io Lease election so extra replicas stay passive."""

    def __init__(self, api, namespace, name, identity, duration_seconds):
        self.api = api
        self.identity = identity
        self.duration = duration_seconds
        self.path = (
            f"/apis/coordination.k8s.io/v1/namespaces/"
            f"{urllib.parse.quote(namespace, safe='')}/leases/{urllib.parse.quote(name, safe='')}"
        )
        self.collection = self.path.rsplit("/", 1)[0]
        self.name = name
        self.namespace = namespace

    def _body(self, resource_version=None, transitions=0):
        body = {
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {"name": self.name, "namespace": self.namespace},
            "spec": {
                "holderIdentity": self.identity,
                "leaseDurationSeconds": self.duration,
                "renewTime": utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "leaseTransitions": transitions,
            },
        }
        if resource_version:
            body["metadata"]["resourceVersion"] = resource_version
        return body

    def try_acquire(self):
        try:
            lease = self.api.get(self.path)
        except ApiError as error:
            if error.status != 404:
                raise
            try:
                self.api.request("POST", self.collection, self._body())
                return True
            except ApiError as create_error:
                if create_error.status == 409:
                    return False
                raise
        spec = lease.get("spec", {})
        holder = spec.get("holderIdentity")
        renew = spec.get("renewTime")
        expired = True
        if renew:
            expired = utcnow() - parse_time(renew) > dt.timedelta(
                seconds=spec.get("leaseDurationSeconds", self.duration)
            )
        if holder not in (None, "", self.identity) and not expired:
            return False
        transitions = spec.get("leaseTransitions", 0) + (0 if holder == self.identity else 1)
        try:
            self.api.request(
                "PUT", self.path, self._body(lease["metadata"].get("resourceVersion"), transitions)
            )
            return True
        except ApiError as error:
            if error.status == 409:
                return False
            raise

    def release(self):
        """Hand the lease over immediately on shutdown instead of waiting for expiry."""
        try:
            lease = self.api.get(self.path)
        except ApiError:
            return
        if lease.get("spec", {}).get("holderIdentity") != self.identity:
            return
        body = self._body(lease["metadata"].get("resourceVersion"), lease["spec"].get("leaseTransitions", 0))
        body["spec"]["holderIdentity"] = ""
        try:
            self.api.request("PUT", self.path, body)
        except ApiError as error:
            log("lease_release_error", error=str(error))


class Controller:
    def __init__(self, api):
        self.api = api
        self.enable_purge = os.environ.get("ENABLE_PURGE", "false").lower() == "true"
        self.allowed_namespaces = split_csv(os.environ.get("ALLOWED_NAMESPACES", ""))
        if not self.allowed_namespaces:
            raise ValueError("ALLOWED_NAMESPACES must contain at least one namespace")
        self.label_key = managed_label_key()
        self.label_values = managed_label_values()
        if not self.label_values:
            raise ValueError("MANAGED_NODE_LABEL_VALUES must contain at least one Node label value")
        self.emit_events = os.environ.get("EMIT_EVENTS", "true").lower() == "true"
        self.group_key = os.environ.get("NODE_GROUP_LABEL_KEY", DEFAULT_NODE_GROUP_LABEL_KEY)
        self.max_in_flight = int(os.environ.get("MAX_CONCURRENT_PER_NODE_GROUP", "1"))
        self.min_ready = int(os.environ.get("MIN_READY_NODES_PER_NODE_GROUP", "0"))
        if self.max_in_flight < 0 or self.min_ready < 0:
            raise ValueError("MAX_CONCURRENT_PER_NODE_GROUP and MIN_READY_NODES_PER_NODE_GROUP must be >= 0")

    # ----- status and events -------------------------------------------------

    def request_path(self, namespace, name):
        return (
            f"/apis/{API_GROUP}/{API_VERSION}/namespaces/"
            f"{urllib.parse.quote(namespace, safe='')}/{RESOURCE}/{urllib.parse.quote(name, safe='')}"
        )

    def status(self, namespace, name, values):
        self.api.merge_patch(f"{self.request_path(namespace, name)}/status", {"status": values})

    def event(self, remediation, reason, message, warning=False):
        if not self.emit_events:
            return
        metadata = remediation["metadata"]
        namespace = metadata["namespace"]
        body = {
            "apiVersion": "v1",
            "kind": "Event",
            "metadata": {"generateName": f"{metadata['name']}.", "namespace": namespace},
            "involvedObject": {
                "apiVersion": f"{API_GROUP}/{API_VERSION}",
                "kind": KIND,
                "name": metadata["name"],
                "namespace": namespace,
                "uid": metadata.get("uid"),
            },
            "reason": reason,
            "message": message[:1024],
            "type": "Warning" if warning else "Normal",
            "source": {"component": COMPONENT},
            "firstTimestamp": format_time(utcnow()),
            "lastTimestamp": format_time(utcnow()),
            "count": 1,
        }
        try:
            self.api.request("POST", f"/api/v1/namespaces/{urllib.parse.quote(namespace, safe='')}/events", body)
        except ApiError as error:
            log("event_error", remediation=metadata["name"], error=str(error))

    # ----- fencing -------------------------------------------------------------

    def validate_node(self, node, expected_uid):
        if node.get("metadata", {}).get("uid") != expected_uid:
            raise ValueError("Node UID does not match remediation fence")
        if not node_is_managed(node, self.label_key, self.label_values):
            raise ValueError("Target does not satisfy the managed node allowlist")
        return node

    def fetch_validated_node(self, spec, expected_uid):
        path = f"/api/v1/nodes/{urllib.parse.quote(spec['nodeName'], safe='')}"
        return self.validate_node(self.api.get(path), expected_uid)

    def fetch_current_remediation(self, metadata):
        current = self.api.get(self.request_path(metadata["namespace"], metadata["name"]))
        current_metadata = current.get("metadata", {})
        if current_metadata.get("uid") != metadata.get("uid"):
            raise ValueError("Remediation UID changed before workload mutation")
        if current_metadata.get("generation") != metadata.get("generation"):
            raise ValueError("Remediation generation changed before workload mutation")
        if current.get("spec", {}).get("approved") is not True:
            raise ValueError("Remediation approval was revoked before workload mutation")
        return current

    def cordon(self, node, expected_uid):
        self.validate_node(node, expected_uid)
        metadata = node["metadata"]
        if node.get("spec", {}).get("unschedulable") is True:
            return False
        label_value = metadata["labels"][self.label_key]
        path = f"/api/v1/nodes/{urllib.parse.quote(metadata['name'], safe='')}"
        patch = [
            {"op": "test", "path": "/metadata/uid", "value": expected_uid},
            {"op": "test", "path": f"/metadata/labels/{json_pointer(self.label_key)}", "value": label_value},
            {"op": "add", "path": "/spec/unschedulable", "value": True},
        ]
        self.api.json_patch(path, patch)
        return True

    # ----- pod access ----------------------------------------------------------

    def pods_on_node(self, node_name):
        selector = urllib.parse.quote(f"spec.nodeName={node_name}", safe="")
        return self.api.get(f"/api/v1/pods?fieldSelector={selector}").get("items", [])

    def all_pods(self):
        return self.api.get("/api/v1/pods").get("items", [])

    def evict(self, pod):
        ref = pod_ref(pod)
        namespace = urllib.parse.quote(ref["namespace"], safe="")
        name = urllib.parse.quote(ref["name"], safe="")
        body = {
            "apiVersion": "policy/v1",
            "kind": "Eviction",
            "metadata": {"name": ref["name"], "namespace": ref["namespace"]},
            "deleteOptions": {"preconditions": {"uid": ref["uid"]}},
        }
        return self.api.request("POST", f"/api/v1/namespaces/{namespace}/pods/{name}/eviction", body)

    def force_delete(self, pod):
        ref = pod_ref(pod)
        namespace = urllib.parse.quote(ref["namespace"], safe="")
        name = urllib.parse.quote(ref["name"], safe="")
        body = {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "gracePeriodSeconds": 0,
            "preconditions": {"uid": ref["uid"]},
        }
        return self.api.request("DELETE", f"/api/v1/namespaces/{namespace}/pods/{name}", body)

    def targets(self, spec, status=None):
        """Resolve target pods. Once selectedPods is persisted it is the only source of
        truth for the lifetime of the DrainRequest; replacements are never adopted."""
        status = status or {}
        prior = status.get("selectedPods", [])
        if prior:
            wanted = {(item["namespace"], item["name"], item["uid"]) for item in prior}
            return [
                pod for pod in self.all_pods()
                if (
                    pod.get("metadata", {}).get("namespace"),
                    pod.get("metadata", {}).get("name"),
                    pod.get("metadata", {}).get("uid"),
                ) in wanted
                and is_actionable_pod(pod)
            ]

        configured = spec.get("podsToDrain", {})
        if configured:
            wanted_names = {
                (namespace, name)
                for namespace, names in configured.items()
                if allowed_namespace(namespace, self.allowed_namespaces)
                for name in names
            }
            return [
                pod for pod in self.all_pods()
                if (
                    pod.get("metadata", {}).get("namespace"),
                    pod.get("metadata", {}).get("name"),
                ) in wanted_names
                and is_actionable_pod(pod)
            ]

        node_pods = self.pods_on_node(spec["nodeName"])
        group_policy = spec.get("groupPolicy", "NodeLocal")
        all_pods = self.all_pods() if group_policy == "Gang" else node_pods
        return select_targets(node_pods, all_pods, group_policy, self.allowed_namespaces)

    # ----- reconciliation ------------------------------------------------------

    def reconcile(self, remediation):
        metadata = remediation["metadata"]
        spec = remediation["spec"]
        current_status = remediation.get("status", {})
        name = metadata["name"]
        namespace = metadata["namespace"]
        generation = metadata.get("generation")

        # Execution state (node fence, selected pod UIDs, deadline) is sticky for the
        # lifetime of the object. A spec edit bumps generation but never re-selects
        # targets or re-fences a different Node; the CRD makes those fields immutable.
        sticky = {
            key: current_status[key]
            for key in ("nodeUID", "selectedPods", "deadlineAt")
            if key in current_status
        }

        def report(values):
            written = {"observedGeneration": generation, **sticky, **values}
            self.status(namespace, name, written)
            # Mirror onto the in-memory object so later requests in the same pass see
            # this one as admitted / in flight for the node-group gates.
            remediation.setdefault("status", {}).update(written)

        if spec.get("approved") is not True:
            report({"phase": "AwaitingApproval", "message": "spec.approved is not true"})
            return

        action = effective_action(spec)
        node_name = spec["nodeName"]
        node_path = f"/api/v1/nodes/{urllib.parse.quote(node_name, safe='')}"
        try:
            node = self.api.get(node_path)
        except ApiError as error:
            if error.status == 404:
                report({"phase": "NodeNotFound", "effectiveAction": action})
                return
            raise

        expected_uid = spec.get("nodeUID") or sticky.get("nodeUID")
        if expected_uid:
            sticky["nodeUID"] = expected_uid
        else:
            if not node_is_managed(node, self.label_key, self.label_values):
                raise ValueError("Target does not satisfy the managed node allowlist")
            sticky["nodeUID"] = node["metadata"]["uid"]
            report({
                "phase": "NodeLocked",
                "effectiveAction": action,
                "message": "Node UID persisted before cordon or workload mutation",
            })
            return

        changed = self.cordon(node, expected_uid)
        if changed:
            log("cordoned", remediation=name, node=node_name)
            self.event(remediation, "Cordoned", f"Cordoned node {node_name} (uid {expected_uid})")

        if action == "PRESERVE":
            report({
                "phase": "Preserving",
                "effectiveAction": action,
                "message": "Node cordoned; workloads remain until natural completion",
            })
            return

        targets = self.targets(spec, sticky)
        prior_selected = sticky.get("selectedPods", [])
        selected = prior_selected or [pod_ref(pod) for pod in targets]
        if not targets:
            sticky["selectedPods"] = selected
            report({
                "phase": "Completed",
                "effectiveAction": action,
                "conditions": [{
                    "type": DRAIN_COMPLETE,
                    "status": "True",
                    "reason": "SelectedPodsGone",
                    "message": "Selected workload UIDs are gone",
                    "observedGeneration": generation,
                    "lastTransitionTime": format_time(utcnow()),
                }],
                "message": "Selected workload UIDs are gone; the customer capacity layer owns node reclaim",
            })
            self.event(remediation, "DrainComplete", f"All selected pods left node {node_name}")
            return

        deadline_text = sticky.get("deadlineAt")
        if deadline_text:
            deadline = parse_time(deadline_text)
        else:
            deadline = utcnow() + dt.timedelta(seconds=spec.get("deadlineSeconds", 300))
            deadline_text = format_time(deadline)

        if not prior_selected or not sticky.get("deadlineAt"):
            sticky["selectedPods"] = selected
            sticky["deadlineAt"] = deadline_text
            report({
                "phase": "TargetsLocked",
                "effectiveAction": action,
                "message": "Target pod UIDs and deadline persisted; no eviction submitted yet",
            })
            self.event(
                remediation, "TargetsLocked",
                f"Selected {len(selected)} pod(s) for {action}; deadline {deadline_text}",
            )
            return

        if utcnow() < deadline:
            blocked, submitted = [], []
            for pod in targets:
                ref = pod_ref(pod)
                try:
                    self.fetch_current_remediation(metadata)
                    self.fetch_validated_node(spec, expected_uid)
                    self.evict(pod)
                    submitted.append(ref)
                except ApiError as error:
                    if error.status == 404:
                        continue
                    if error.status == 429:
                        blocked.append(ref)
                        continue
                    raise
            phase = "DrainBlocked" if blocked else "Evicting"
            report({
                "phase": phase,
                "effectiveAction": action,
                "evictionsSubmitted": submitted,
                "pdbBlocked": blocked,
            })
            if blocked and current_status.get("phase") != "DrainBlocked":
                self.event(
                    remediation, "DrainBlocked",
                    f"PodDisruptionBudget blocked eviction of {len(blocked)} pod(s)", warning=True,
                )
            return

        if action == "DRAIN":
            report({
                "phase": "TimedOut",
                "effectiveAction": action,
                "message": "Drain deadline expired; PDB and grace were not bypassed",
            })
            self.event(remediation, "TimedOut", "Drain deadline expired without escalation", warning=True)
            return

        if not purge_authorized(spec, self.enable_purge):
            report({
                "phase": "PurgeNotAuthorized",
                "effectiveAction": action,
                "message": (
                    "Forced deletion requires fatal/integrity fault, spec.approved, "
                    "allowForceAfterDeadline, ENABLE_PURGE, and namespace delete RBAC"
                ),
            })
            self.event(remediation, "PurgeNotAuthorized", "PURGE gates not satisfied", warning=True)
            return

        deleted = []
        for pod in targets:
            ref = pod_ref(pod)
            try:
                self.fetch_current_remediation(metadata)
                self.fetch_validated_node(spec, expected_uid)
                self.force_delete(pod)
                deleted.append(ref)
            except ApiError as error:
                if error.status != 404:
                    raise
        report({
            "phase": "Purging",
            "effectiveAction": action,
            "forcedDeletes": deleted,
            "fencingVerified": False,
            "message": "Pod API objects deleted; physical process fencing is not proven",
        })
        if deleted:
            self.event(
                remediation, "ForcedDelete",
                f"Force-deleted {len(deleted)} pod UID(s); fencingVerified=false", warning=True,
            )

    def managed_nodes(self):
        selector = urllib.parse.quote(self.label_key, safe="")
        items = self.api.get(f"/api/v1/nodes?labelSelector={selector}").get("items", [])
        return {item["metadata"]["name"]: item for item in items}

    def block(self, remediation, reason, message):
        metadata = remediation["metadata"]
        if remediation.get("status", {}).get("message") == message:
            return
        try:
            self.status(metadata["namespace"], metadata["name"], {
                "phase": "Blocked",
                "blockedReason": reason,
                "message": message,
                "observedGeneration": metadata.get("generation"),
            })
            self.event(remediation, reason, message)
        except Exception as status_error:
            log("status_error", remediation=metadata["name"], error=str(status_error))

    def run_once(self):
        path = f"/apis/{API_GROUP}/{API_VERSION}/{RESOURCE}"
        remediations = sorted(self.api.get(path).get("items", []), key=request_sort_key)
        owners = node_owners(remediations)
        nodes_by_name = None  # fetched lazily, once per pass, only if a gate needs it
        for remediation in remediations:
            metadata = remediation["metadata"]
            if is_terminal(remediation):
                continue
            name = metadata["name"]
            namespace = metadata["namespace"]
            node_name = remediation.get("spec", {}).get("nodeName")
            owner = owners.get(node_name)
            if owner is not None and owner["metadata"].get("uid") != metadata.get("uid"):
                self.block(remediation, "NodeOwned", (
                    f"Node {node_name} is owned by DrainRequest "
                    f"{owner['metadata']['namespace']}/{owner['metadata']['name']} until it is terminal"
                ))
                continue
            if (
                not is_admitted(remediation)
                and remediation.get("spec", {}).get("approved") is True
                and (self.max_in_flight > 0 or self.min_ready > 0)
            ):
                try:
                    if nodes_by_name is None:
                        nodes_by_name = self.managed_nodes()
                    verdict = node_group_gate(
                        node_name, nodes_by_name, remediations, self.group_key, self.label_key,
                        self.max_in_flight, self.min_ready,
                    )
                except Exception as error:
                    log("gate_error", remediation=name, error=str(error))
                    verdict = ("GateError", f"Node group gate could not be evaluated: {error}")
                if verdict is not None:
                    self.block(remediation, *verdict)
                    continue
            try:
                self.reconcile(remediation)
            except Exception as error:
                log("reconcile_error", remediation=name, error=str(error))
                self.event(remediation, "ReconcileError", str(error), warning=True)
                try:
                    self.status(namespace, name, {
                        "phase": "Error",
                        "message": str(error),
                        "observedGeneration": metadata.get("generation"),
                    })
                except Exception as status_error:
                    log("status_error", remediation=name, error=str(status_error))


def main():
    poll_seconds = int(os.environ.get("POLL_SECONDS", "5"))
    api = KubeClient()
    controller = Controller(api)
    lease = None
    if os.environ.get("LEADER_ELECTION", "true").lower() == "true":
        lease = LeaderLease(
            api,
            os.environ.get("POD_NAMESPACE", "node-remediation-system"),
            os.environ.get("LEASE_NAME", COMPONENT),
            os.environ.get("POD_NAME") or socket.gethostname(),
            int(os.environ.get("LEASE_DURATION_SECONDS", "30")),
        )
    log(
        "started",
        enable_purge=controller.enable_purge,
        allowed_namespaces=sorted(controller.allowed_namespaces),
        managed_label_key=controller.label_key,
        managed_label_values=sorted(controller.label_values),
        leader_election=lease is not None,
    )
    leading = False
    stopping = {"flag": False}

    def request_stop(signum, _frame):
        log("stopping", signal=signum)
        stopping["flag"] = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        while not stopping["flag"]:
            try:
                if lease is None or lease.try_acquire():
                    if not leading:
                        log("leading", identity=lease.identity if lease else "single")
                        leading = True
                    controller.run_once()
                elif leading:
                    log("lost_lease")
                    leading = False
            except Exception as error:
                log("loop_error", error=str(error))
            for _ in range(poll_seconds * 10):
                if stopping["flag"]:
                    break
                time.sleep(0.1)
    finally:
        if lease is not None and leading:
            lease.release()
            log("lease_released", identity=lease.identity)


if __name__ == "__main__":
    main()
    sys.exit(0)

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "remediation_controller", ROOT / "src/controller/remediation_controller.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

NS = "workload-a"
REQUEST_NS = "node-remediation-system"
REQUEST_PATH = f"/apis/nvsentinel.nvidia.com/v1alpha1/namespaces/{REQUEST_NS}/drainrequests/case-1"


@pytest.fixture(autouse=True)
def controller_env(monkeypatch):
    monkeypatch.setenv("ALLOWED_NAMESPACES", NS)
    monkeypatch.setenv("MANAGED_NODE_LABEL_KEY", "workload")
    monkeypatch.setenv("MANAGED_NODE_LABEL_VALUES", "managed-gpu,managed-cpu")
    monkeypatch.delenv("ENABLE_PURGE", raising=False)
    monkeypatch.setenv("EMIT_EVENTS", "false")


def pod(name="worker-0", namespace=NS, gpu=True, group="job-1", owner="Job", phase="Running"):
    resources = {"limits": {module.GPU_RESOURCE: "1"}} if gpu else {}
    annotations = {module.GROUP_ANNOTATION: group} if group else {}
    return {
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": f"uid-{name}",
            "annotations": annotations,
            "ownerReferences": [{"kind": owner}],
        },
        "spec": {"containers": [{"name": "main", "resources": resources}]},
        "status": {"phase": phase},
    }


def node(uid="node-uid", unschedulable=False, workload="managed-gpu"):
    return {
        "metadata": {"name": "gpu-node", "uid": uid, "labels": {"workload": workload}},
        "spec": {"unschedulable": unschedulable},
    }


def remediation(status=None, generation=1, name="case-1", uid="remediation-uid", created="2026-01-01T00:00:00Z", **spec_overrides):
    value = {
        "metadata": {
            "name": name,
            "namespace": REQUEST_NS,
            "uid": uid,
            "generation": generation,
            "creationTimestamp": created,
        },
        "spec": {
            "nodeName": "gpu-node",
            "nodeUID": "node-uid",
            "faultClass": "Recoverable",
            "action": "DRAIN",
            "approved": True,
            "deadlineSeconds": 900,
            "groupPolicy": "NodeLocal",
            **spec_overrides,
        },
    }
    if status is not None:
        value["status"] = status
    return value


# ----- pure policy helpers -----------------------------------------------------

def test_default_actions_are_conservative():
    assert module.effective_action({"faultClass": "Unknown"}) == "PRESERVE"
    assert module.effective_action({"faultClass": "Recoverable"}) == "DRAIN"
    assert module.effective_action({"faultClass": "Fatal"}) == "DRAIN"
    assert module.effective_action({"faultClass": "Integrity"}) == "PURGE"


def test_managed_node_uses_configured_label():
    values = {"managed-gpu", "managed-cpu"}
    assert module.node_is_managed(node(), "workload", values)
    assert module.node_is_managed(node(workload="managed-cpu"), "workload", values)
    assert not module.node_is_managed(node(workload="scale-test"), "workload", values)
    assert not module.node_is_managed({"metadata": {"labels": {}}}, "workload", values)


def test_controller_requires_label_values(monkeypatch):
    monkeypatch.setenv("MANAGED_NODE_LABEL_VALUES", "")
    monkeypatch.delenv("MANAGED_NODE_WORKLOADS", raising=False)
    with pytest.raises(ValueError, match="MANAGED_NODE_LABEL_VALUES"):
        module.Controller(FakeApi())


def test_empty_namespace_allowlist_fails_closed(monkeypatch):
    monkeypatch.setenv("ALLOWED_NAMESPACES", "")
    with pytest.raises(ValueError, match="ALLOWED_NAMESPACES"):
        module.Controller(FakeApi())


def test_daemonset_mirror_and_terminal_pods_are_not_actionable():
    assert not module.is_actionable_pod(pod(owner="DaemonSet"))
    assert not module.is_actionable_pod(pod(phase="Succeeded"))
    assert not module.is_actionable_pod(pod(phase="Failed"))
    mirror = pod()
    mirror["metadata"]["annotations"]["kubernetes.io/config.mirror"] = "x"
    assert not module.is_actionable_pod(mirror)
    assert module.is_actionable_pod(pod())


def test_gang_selection_expands_across_nodes():
    local, peer = pod("worker-0"), pod("worker-1")
    coordinator = pod("coordinator", gpu=False)
    unrelated = pod("other", group="other-job")
    outside_ns = pod("outside", namespace="other-ns")
    selected = module.select_targets([local], [local, peer, coordinator, unrelated, outside_ns], "Gang", {NS})
    assert {item["metadata"]["name"] for item in selected} == {"worker-0", "worker-1", "coordinator"}


def test_gang_selection_without_contract_fails_closed():
    with pytest.raises(ValueError, match="explicit group-id"):
        module.select_targets([pod(group=None)], [pod(group=None)], "Gang", {NS})
    with pytest.raises(ValueError, match="explicit group-id"):
        module.select_targets([pod("a", group="g1"), pod("b", group="g2")], [], "Gang", {NS})


def test_node_local_targets_only_allowed_namespaces():
    local_gpu, local_cpu = pod("worker"), pod("coordinator", gpu=False)
    foreign = pod("foreign", namespace="kube-system")
    selected = module.select_targets([local_gpu, local_cpu, foreign], [], "NodeLocal", {NS})
    assert [item["metadata"]["name"] for item in selected] == ["worker", "coordinator"]


def test_purge_requires_every_gate():
    base = {"faultClass": "Integrity", "action": "PURGE", "approved": True, "allowForceAfterDeadline": True}
    assert module.purge_authorized(base, True)
    for field in ("approved", "allowForceAfterDeadline"):
        assert not module.purge_authorized(dict(base, **{field: False}), True)
    assert not module.purge_authorized(base, False)
    assert not module.purge_authorized(dict(base, faultClass="Recoverable"), True)
    assert not module.purge_authorized(dict(base, action="DRAIN"), True)


def test_node_owners_picks_oldest_non_terminal():
    older = remediation(name="older", uid="u1", created="2026-01-01T00:00:00Z")
    newer = remediation(name="newer", uid="u2", created="2026-01-01T00:01:00Z")
    done = remediation(name="done", uid="u3", created="2025-12-31T00:00:00Z",
                       status={"phase": "Completed", "observedGeneration": 1})
    owners = module.node_owners([newer, older, done])
    assert owners["gpu-node"]["metadata"]["name"] == "older"


# ----- fakes -------------------------------------------------------------------

class FakeApi:
    def __init__(self):
        self.patches = []
        self.requests = []

    def json_patch(self, path, body):
        self.patches.append((path, body))

    def request(self, method, path, body=None, content_type="application/json"):
        self.requests.append((method, path, body))
        return {}


class ReconcileApi(FakeApi):
    def __init__(self, workload_pods):
        super().__init__()
        self.node = node()
        self.workload_pods = workload_pods
        self.statuses = []
        self.node_gets = 0
        self.replace_after_node_get = None
        self.current_remediation = None
        self.evict_status = None  # e.g. 429 to simulate PDB rejection

    def get(self, path):
        if path == REQUEST_PATH:
            return self.current_remediation
        if path == "/api/v1/nodes/gpu-node":
            self.node_gets += 1
            if self.replace_after_node_get is not None and self.node_gets > self.replace_after_node_get:
                return node(uid="replacement-uid", unschedulable=True)
            return self.node
        if path.startswith("/api/v1/pods?fieldSelector=") or path == "/api/v1/pods":
            return {"items": self.workload_pods}
        raise AssertionError(path)

    def json_patch(self, path, body):
        super().json_patch(path, body)
        self.node["spec"]["unschedulable"] = True

    def merge_patch(self, path, body):
        self.statuses.append(body["status"])

    def request(self, method, path, body=None, content_type="application/json"):
        if self.evict_status and path.endswith("/eviction"):
            raise module.ApiError(self.evict_status, "blocked")
        return super().request(method, path, body, content_type)


def locked_status(*pods, deadline="2099-01-01T00:00:00Z", generation=1, node_uid="node-uid"):
    return {
        "observedGeneration": generation,
        "phase": "TargetsLocked",
        "nodeUID": node_uid,
        "deadlineAt": deadline,
        "selectedPods": [module.pod_ref(p) for p in pods],
    }


# ----- cordon fencing ----------------------------------------------------------

def test_cordon_is_uid_fenced_and_label_gated():
    api = FakeApi()
    controller = module.Controller(api)
    assert controller.cordon(node(workload="managed-cpu"), "node-uid") is True
    path, patch = api.patches[0]
    assert path == "/api/v1/nodes/gpu-node"
    assert patch[0] == {"op": "test", "path": "/metadata/uid", "value": "node-uid"}
    assert patch[1] == {"op": "test", "path": "/metadata/labels/workload", "value": "managed-cpu"}
    assert patch[2] == {"op": "add", "path": "/spec/unschedulable", "value": True}
    with pytest.raises(ValueError, match="UID"):
        controller.cordon(node(), "replacement-uid")
    with pytest.raises(ValueError, match="allowlist"):
        controller.cordon(node(workload="scale-test"), "node-uid")
    assert controller.cordon(node(unschedulable=True), "node-uid") is False


def test_cordon_escapes_label_key_json_pointer(monkeypatch):
    monkeypatch.setenv("MANAGED_NODE_LABEL_KEY", "example.com/tier")
    monkeypatch.setenv("MANAGED_NODE_LABEL_VALUES", "gpu")
    api = FakeApi()
    target = node()
    target["metadata"]["labels"] = {"example.com/tier": "gpu"}
    module.Controller(api).cordon(target, "node-uid")
    assert api.patches[0][1][1]["path"] == "/metadata/labels/example.com~1tier"


# ----- reconcile lifecycle -------------------------------------------------------

def test_reconcile_persists_targets_before_eviction():
    api = ReconcileApi([pod("worker")])
    module.Controller(api).reconcile(remediation())
    last = api.statuses[-1]
    assert last["phase"] == "TargetsLocked"
    assert last["selectedPods"][0]["uid"] == "uid-worker"
    assert last["nodeUID"] == "node-uid"
    assert "deadlineAt" in last
    assert api.requests == []


def test_reconcile_evicts_only_after_durable_lock():
    api = ReconcileApi([pod("worker")])
    controller = module.Controller(api)
    controller.reconcile(remediation())
    second = remediation(status=api.statuses[-1])
    api.current_remediation = second
    controller.reconcile(second)
    assert api.statuses[-1]["phase"] == "Evicting"
    method, path, body = api.requests[0]
    assert (method, path) == ("POST", f"/api/v1/namespaces/{NS}/pods/worker/eviction")
    assert body["deleteOptions"]["preconditions"]["uid"] == "uid-worker"


def test_new_generation_keeps_fence_selection_and_deadline():
    """A spec edit (generation bump) must never re-select targets or re-fence."""
    old_worker = pod("old-worker")
    api = ReconcileApi([pod("replacement"), old_worker])
    old = dict(locked_status(old_worker, generation=1), phase="Evicting")
    current = remediation(status=old, generation=2)
    api.current_remediation = current
    module.Controller(api).reconcile(current)
    last = api.statuses[-1]
    assert last["observedGeneration"] == 2
    assert [p["uid"] for p in last["selectedPods"]] == ["uid-old-worker"]
    assert last["deadlineAt"] == "2099-01-01T00:00:00Z"
    assert last["nodeUID"] == "node-uid"
    assert [r[1] for r in api.requests] == [f"/api/v1/namespaces/{NS}/pods/old-worker/eviction"]


def test_status_fence_is_used_when_spec_has_no_node_uid():
    api = ReconcileApi([pod("worker")])
    request = remediation(nodeUID=None)
    del request["spec"]["nodeUID"]
    controller = module.Controller(api)
    controller.reconcile(request)
    assert api.statuses[-1]["phase"] == "NodeLocked"
    assert api.statuses[-1]["nodeUID"] == "node-uid"
    assert api.patches == []  # not even cordoned yet
    # Node replaced with the same name before the next pass: fence must hold.
    api.node = node(uid="replacement-uid")
    request_with_status = dict(request, status=dict(api.statuses[-1], observedGeneration=2))
    request_with_status["metadata"]["generation"] = 2
    with pytest.raises(ValueError, match="UID"):
        controller.reconcile(request_with_status)
    assert api.patches == []


def test_completed_sets_drain_complete_condition():
    worker = pod("worker")
    api = ReconcileApi([])  # selected pod is gone
    current = remediation(status=locked_status(worker))
    api.current_remediation = current
    module.Controller(api).reconcile(current)
    last = api.statuses[-1]
    assert last["phase"] == "Completed"
    condition = last["conditions"][0]
    assert (condition["type"], condition["status"]) == ("DrainComplete", "True")
    assert last["selectedPods"][0]["uid"] == "uid-worker"


def test_pdb_rejection_reports_drain_blocked_without_escalation():
    worker = pod("worker")
    api = ReconcileApi([worker])
    api.evict_status = 429
    current = remediation(status=locked_status(worker))
    api.current_remediation = current
    module.Controller(api).reconcile(current)
    assert api.statuses[-1]["phase"] == "DrainBlocked"
    assert api.statuses[-1]["pdbBlocked"][0]["uid"] == "uid-worker"
    assert all(not r[1].endswith("/pods/worker") for r in api.requests)


def test_drain_deadline_expiry_times_out_and_never_force_deletes():
    worker = pod("worker")
    api = ReconcileApi([worker])
    current = remediation(status=locked_status(worker, deadline="2000-01-01T00:00:00Z"))
    api.current_remediation = current
    module.Controller(api).reconcile(current)
    assert api.statuses[-1]["phase"] == "TimedOut"
    assert api.requests == []


def test_purge_after_deadline_requires_all_gates(monkeypatch):
    worker = pod("worker")
    api = ReconcileApi([worker])
    current = remediation(
        status=locked_status(worker, deadline="2000-01-01T00:00:00Z"),
        faultClass="Integrity", action="PURGE", allowForceAfterDeadline=True,
    )
    api.current_remediation = current
    module.Controller(api).reconcile(current)
    assert api.statuses[-1]["phase"] == "PurgeNotAuthorized"
    assert api.requests == []

    monkeypatch.setenv("ENABLE_PURGE", "true")
    module.Controller(api).reconcile(current)
    last = api.statuses[-1]
    assert last["phase"] == "Purging"
    assert last["fencingVerified"] is False
    method, path, body = api.requests[-1]
    assert (method, path) == ("DELETE", f"/api/v1/namespaces/{NS}/pods/worker")
    assert body["preconditions"]["uid"] == "uid-worker"
    assert body["gracePeriodSeconds"] == 0


def test_preserve_cordons_and_leaves_pods():
    api = ReconcileApi([pod("worker")])
    module.Controller(api).reconcile(remediation(faultClass="Unknown", action="PRESERVE"))
    assert api.statuses[-1]["phase"] == "Preserving"
    assert api.node["spec"]["unschedulable"] is True
    assert api.requests == []


def test_unapproved_request_does_nothing():
    api = ReconcileApi([pod("worker")])
    module.Controller(api).reconcile(remediation(approved=False))
    assert api.statuses[-1]["phase"] == "AwaitingApproval"
    assert api.patches == [] and api.requests == []


def test_node_not_found_is_terminal():
    class MissingNodeApi(ReconcileApi):
        def get(self, path):
            if path == "/api/v1/nodes/gpu-node":
                raise module.ApiError(404, "not found")
            return super().get(path)

    api = MissingNodeApi([])
    module.Controller(api).reconcile(remediation())
    assert api.statuses[-1]["phase"] == "NodeNotFound"


# ----- pre-mutation revalidation -------------------------------------------------

def test_node_replacement_before_eviction_fails_closed():
    worker = pod("worker")
    api = ReconcileApi([worker])
    api.node["spec"]["unschedulable"] = True
    api.replace_after_node_get = 1
    current = remediation(status=locked_status(worker))
    api.current_remediation = current
    with pytest.raises(ValueError, match="UID"):
        module.Controller(api).reconcile(current)
    assert api.requests == []


def test_approval_revocation_before_eviction_fails_closed():
    worker = pod("worker")
    api = ReconcileApi([worker])
    api.node["spec"]["unschedulable"] = True
    snapshot = remediation(status=locked_status(worker))
    api.current_remediation = remediation(status=locked_status(worker), approved=False)
    with pytest.raises(ValueError, match="approval was revoked"):
        module.Controller(api).reconcile(snapshot)
    assert api.requests == []


def test_generation_change_before_eviction_fails_closed():
    worker = pod("worker")
    api = ReconcileApi([worker])
    api.node["spec"]["unschedulable"] = True
    snapshot = remediation(status=locked_status(worker), generation=1)
    api.current_remediation = remediation(status=locked_status(worker), generation=2)
    with pytest.raises(ValueError, match="generation changed"):
        module.Controller(api).reconcile(snapshot)
    assert api.requests == []


def test_remediation_recreation_before_eviction_fails_closed():
    worker = pod("worker")
    api = ReconcileApi([worker])
    api.node["spec"]["unschedulable"] = True
    snapshot = remediation(status=locked_status(worker))
    api.current_remediation = remediation(status=locked_status(worker), uid="new-remediation-uid")
    with pytest.raises(ValueError, match="UID changed"):
        module.Controller(api).reconcile(snapshot)
    assert api.requests == []


def test_node_replacement_between_gang_evictions_stops_batch():
    workers = [pod("worker-0"), pod("worker-1")]
    api = ReconcileApi(workers)
    api.node["spec"]["unschedulable"] = True
    api.replace_after_node_get = 2
    current = remediation(status=locked_status(*workers))
    api.current_remediation = current
    with pytest.raises(ValueError, match="UID"):
        module.Controller(api).reconcile(current)
    assert [r[0] for r in api.requests] == ["POST"]


def test_durable_selection_ignores_replacement_pods():
    peer = pod("worker-1")

    class PodApi(FakeApi):
        def get(self, path):
            assert path == "/api/v1/pods"
            return {"items": [peer, pod("replacement", group="job-1")]}

    controller = module.Controller(PodApi())
    status = {"selectedPods": [module.pod_ref(pod("worker-0")), module.pod_ref(peer)]}
    selected = controller.targets({"nodeName": "gpu-node"}, status)
    assert [item["metadata"]["name"] for item in selected] == ["worker-1"]


def test_pods_to_drain_is_filtered_by_namespace_allowlist():
    class PodApi(FakeApi):
        def get(self, path):
            return {"items": [pod("a"), pod("b", namespace="other")]}

    controller = module.Controller(PodApi())
    spec = {"nodeName": "gpu-node", "podsToDrain": {NS: ["a"], "other": ["b"]}}
    assert [p["metadata"]["name"] for p in controller.targets(spec, {})] == ["a"]


# ----- run_once: per-node ownership and error handling ---------------------------

class RunOnceApi(ReconcileApi):
    def __init__(self, items, workload_pods):
        super().__init__(workload_pods)
        self.items = items
        self.status_patches = []

    def get(self, path):
        if path == "/apis/nvsentinel.nvidia.com/v1alpha1/drainrequests":
            return {"items": self.items}
        if path.endswith("/drainrequests/newer") or path.endswith("/drainrequests/older"):
            return next(i for i in self.items if path.endswith(i["metadata"]["name"]))
        return super().get(path)

    def merge_patch(self, path, body):
        self.status_patches.append((path, body["status"]))


def test_run_once_blocks_second_request_for_same_node():
    older = remediation(name="older", uid="u1", created="2026-01-01T00:00:00Z")
    newer = remediation(name="newer", uid="u2", created="2026-01-01T00:01:00Z")
    api = RunOnceApi([newer, older], [pod("worker")])
    module.Controller(api).run_once()
    by_name = {path.rsplit("/", 2)[-2]: status for path, status in api.status_patches}
    assert by_name["older"]["phase"] == "TargetsLocked"
    assert by_name["newer"]["phase"] == "Blocked"
    assert "owned by DrainRequest node-remediation-system/older" in by_name["newer"]["message"]


def test_run_once_skips_terminal_and_records_errors():
    done = remediation(name="older", uid="u1", status={"phase": "Completed", "observedGeneration": 1})
    broken = remediation(name="newer", uid="u2", created="2026-01-01T00:01:00Z")
    broken["spec"]["groupPolicy"] = "Gang"
    api = RunOnceApi([done, broken], [pod("worker", group=None)])
    module.Controller(api).run_once()
    assert len(api.status_patches) == 1
    path, status = api.status_patches[0]
    assert path.endswith("/drainrequests/newer/status")
    assert status["phase"] == "Error"
    assert "group-id" in status["message"]


# ----- leader election ------------------------------------------------------------

class LeaseApi:
    def __init__(self, existing=None):
        self.lease = existing
        self.calls = []

    def get(self, path):
        if self.lease is None:
            raise module.ApiError(404, "not found")
        return self.lease

    def request(self, method, path, body=None, content_type=None):
        self.calls.append((method, body["spec"]["holderIdentity"]))
        self.lease = dict(body, metadata=dict(body["metadata"], resourceVersion="2"))
        return self.lease


def test_lease_acquired_when_absent_and_renewed_by_holder():
    api = LeaseApi()
    lease = module.LeaderLease(api, REQUEST_NS, "ctl", "pod-a", 30)
    assert lease.try_acquire() is True
    assert api.calls[0][0] == "POST"
    assert lease.try_acquire() is True
    assert api.calls[1] == ("PUT", "pod-a")


def test_lease_held_by_live_peer_is_not_taken():
    fresh = module.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    api = LeaseApi({
        "metadata": {"resourceVersion": "1"},
        "spec": {"holderIdentity": "pod-b", "renewTime": fresh, "leaseDurationSeconds": 30},
    })
    assert module.LeaderLease(api, REQUEST_NS, "ctl", "pod-a", 30).try_acquire() is False
    assert api.calls == []


def test_release_clears_only_own_lease():
    fresh = module.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    api = LeaseApi({
        "metadata": {"resourceVersion": "1"},
        "spec": {"holderIdentity": "pod-a", "renewTime": fresh, "leaseDurationSeconds": 30},
    })
    module.LeaderLease(api, REQUEST_NS, "ctl", "pod-a", 30).release()
    assert api.calls == [("PUT", "")]
    api.calls.clear()
    api.lease["spec"]["holderIdentity"] = "pod-b"
    module.LeaderLease(api, REQUEST_NS, "ctl", "pod-a", 30).release()
    assert api.calls == []


def test_expired_lease_is_taken_over():
    api = LeaseApi({
        "metadata": {"resourceVersion": "1"},
        "spec": {"holderIdentity": "pod-b", "renewTime": "2000-01-01T00:00:00.000000Z",
                 "leaseDurationSeconds": 30, "leaseTransitions": 3},
    })
    assert module.LeaderLease(api, REQUEST_NS, "ctl", "pod-a", 30).try_acquire() is True
    assert api.calls == [("PUT", "pod-a")]
    assert api.lease["spec"]["leaseTransitions"] == 4


# ----- packaged RBAC invariants ---------------------------------------------------

def test_baseline_rbac_cannot_evict_or_delete_cluster_wide():
    baseline = (ROOT / "deploy/kubernetes/rbac/controller-rbac.yaml").read_text()
    evict = (ROOT / "deploy/kubernetes/rbac/workload-namespace-rbac.yaml").read_text()
    purge = (ROOT / "deploy/kubernetes/optional/purge-rbac.yaml").read_text()
    source = (ROOT / "src/controller/remediation_controller.py").read_text()
    assert '"delete"' not in baseline
    assert "pods/eviction" not in baseline
    assert "kind: Role\n" in evict and "pods/eviction" in evict
    assert "kind: Role\n" in purge and 'verbs: ["delete"]' in purge
    assert "REPLACE_WITH_WORKLOAD_NAMESPACE" in purge and "REPLACE_WITH_WORKLOAD_NAMESPACE" in evict
    assert "nodeclaims" not in baseline.lower()
    assert "boto3" not in source
    assert "terminate_instances" not in source


def test_no_lab_identifiers_in_package():
    # Assembled from parts so this file does not match itself.
    forbidden = ("gpu-" + "bench", "scale-test-" + "lab", "lab.k8s-" + "scale-test", "gpu-remediation-" + "system")
    for path in ROOT.rglob("*"):
        if not path.is_file() or "__pycache__" in path.parts or ".git" in path.parts:
            continue
        if path.suffix in {".py", ".yaml", ".md", ".txt"} or path.name.endswith("Dockerfile"):
            text = path.read_text(errors="ignore")
            for token in forbidden:
                assert token not in text, f"{token} found in {path.relative_to(ROOT)}"

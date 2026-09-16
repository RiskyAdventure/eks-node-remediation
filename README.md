# EKS node remediation

Quarantine EC2-backed Amazon EKS nodes when AWS Health reports a planned change or an
active hardware fault, apply an explicit workload policy (wait, drain, or gated purge),
and leave node replacement to the customer's capacity layer. Karpenter is not required
and is not part of this solution.

```
scheduledChange  -> EventBridge -> KMS SQS -> AWS Node Termination Handler (upstream, cordonOnly)
issue            -> EventBridge -> KMS SQS -> aws-health-issue-adapter (custom) -> DrainRequest
GPU-local fault  -> NVSentinel (upstream, optional) --custom-drain--> DrainRequest
DrainRequest     -> node-remediation-controller (custom): cordon, select, evict, deadline, gated purge
workloads        -> Deployments / JobSet RestartJobSet / Volcano RestartJob
node replacement -> Cluster Autoscaler | EKS managed node group repair | ASG actuator (customer)
```

Read `docs/ARCHITECTURE.md`, `docs/PROVENANCE.md`, `docs/PRODUCTION_GAPS.md`, and
`docs/VALIDATION.md` before deploying.

## Repository layout

| Path | Contents |
|---|---|
| `src/controller/remediation_controller.py` | DrainRequest controller (drain plugin). Stdlib only. |
| `src/adapters/aws_health_issue_adapter.py` | AWS Health EC2 issue -> DrainRequest adapter (boto3). |
| `containers/` | Dockerfiles for both images. |
| `deploy/aws/` | CloudFormation: EventBridge rules, KMS, SQS + DLQ + alarms, IAM, Pod Identity. |
| `deploy/kubernetes/` | CRD, RBAC, Deployments. |
| `deploy/helm/nth-values.yaml` | Values for the pinned upstream NTH chart. |
| `integrations/nvsentinel/` | Drain template + node-drainer values for NVSentinel custom-drain mode, plus DCGM host engine and `nvidia` RuntimeClass examples for clusters without the GPU Operator. |
| `integrations/jobset/` | JobSet example with whole-job restart on eviction. |
| `examples/` | Example DrainRequests and a PDB-protected workload. |
| `tests/` | Offline unit tests (`python -m pytest tests -q`). |
| `validation/fixtures/` | Synthetic event generators for staging injection. |

## Configuration

Controller (`deploy/kubernetes/controller-deployment.yaml`):

| Variable | Default | Meaning |
|---|---|---|
| `ALLOWED_NAMESPACES` | required | Namespaces whose pods may be evicted/deleted. Empty fails startup. Grant `rbac/workload-namespace-rbac.yaml` per namespace. |
| `MANAGED_NODE_LABEL_KEY` / `MANAGED_NODE_LABEL_VALUES` | `workload` / required | Only Nodes carrying one of these label values may be cordoned or drained. Empty values fail startup. The label is re-tested atomically in the cordon patch. |
| `ENABLE_PURGE` | `false` | Process-level gate for force deletion. Also requires `optional/purge-rbac.yaml` in the namespace and a request with `action: PURGE`, `allowForceAfterDeadline: true`, `approved: true`, faultClass `Fatal` or `Integrity`. |
| `NODE_GROUP_LABEL_KEY` | `eks.amazonaws.com/nodegroup` | Node label that identifies a node group for the budgets below. Nodes without it fall back to their managed label value. |
| `MAX_CONCURRENT_PER_NODE_GROUP` | `1` | At most this many nodes of one node group may be in flight (`NodeLocked` through `Purging`) at once. Later requests wait in `Blocked` (`blockedReason: NodeGroupBudget`) and are admitted oldest-first as others finish. `0` disables. |
| `MIN_READY_NODES_PER_NODE_GROUP` | `0` | Do not admit a request unless the node group would still have at least this many Ready, schedulable, uninvolved nodes. Paces drains to the capacity layer's replacements (`blockedReason: NodeGroupCapacity`). `0` disables; `2` is a sensible production start for groups of three or more. |
| `LEADER_ELECTION` / `LEASE_DURATION_SECONDS` | `true` / `30` | Lease in the controller namespace; extra replicas are passive. |
| `POLL_SECONDS` | `5` | Reconcile interval. |
| `EMIT_EVENTS` | `true` | Kubernetes Events on the DrainRequest. |

Adapter (`deploy/kubernetes/aws-adapter-deployment.yaml`):

| Variable | Default | Meaning |
|---|---|---|
| `EXPECTED_ACCOUNT`, `EXPECTED_REGION`, `QUEUE_URL` | required | Envelope must match; `detail.affectedAccount` / `detail.eventRegion` must match when present. |
| `OBSERVE_ONLY` | `true` | Log the would-be DrainRequest, create nothing. |
| `ENABLED_DRAIN_EVENT_CODES` | empty | Event codes allowed to act. Codes with a classification in `POLICIES` drain; other enabled codes PRESERVE (cordon only). |
| `UNKNOWN_EVENT_CODE_ACTION` | `ignore` | `ignore`: codes not enabled create nothing. `preserve`: cordon-only request. |
| `SCHEDULED_CHANGE_ACTION` | `ignore` | `drain`: also consume EC2 `scheduledChange` events (retirement, host maintenance) and create one `Recoverable/DRAIN` request per node, so retiring nodes are emptied under the node-group budget before EC2 acts instead of all hard-stopping at the scheduled minute. Needs the stack's `ScheduledChangeRuleState=ENABLED`. NTH keeps cordoning independently. |
| `SCHEDULED_CHANGE_MARGIN_SECONDS` | `1800` | The scheduled-change drain deadline ends this long before `detail.startTime` (clamped to the CRD's 60 s to 3600 s range; unknown start uses 3600). |
| `UNMAPPED_INSTANCE_POLICY` | `skip` | `skip`: instances without a managed Node are logged and acknowledged. `fail`: the whole message retries and dead-letters. Ambiguous providerID matches always fail. |
| `MAX_INSTANCES_PER_EVENT` | `25` | Blast-radius cap per event. |
| `ALLOWED_SOURCES` | `aws.health` | Add the synthetic source only in staging. |
| `MANAGED_NODE_LABEL_KEY` / `MANAGED_NODE_LABEL_VALUES` | `workload` / required | Same allowlist as the controller. |

DrainRequest names are `aws-health-<sha256(eventArn:instanceId)[:20]>`, so Health
updates, entity pagination, and duplicate SQS delivery converge on one object.

## DrainRequest lifecycle

`AwaitingApproval` -> `NodeLocked` (UID persisted) -> cordon -> `Preserving` |
`TargetsLocked` (pod UIDs + deadline persisted) -> `Evicting` / `DrainBlocked` ->
`Completed` (`DrainComplete=True`) | `TimedOut` (DRAIN) | `PurgeNotAuthorized` /
`Purging` -> `Completed` (PURGE). `Blocked` carries `status.blockedReason`:
`NodeOwned` (another non-terminal request owns the Node), `NodeGroupBudget` (too many
nodes of this node group in flight), or `NodeGroupCapacity` (the group would drop
below its ready-node floor); blocked requests are re-evaluated every poll and admitted
oldest-first. `NodeNotFound`, `TimedOut`, `PurgeNotAuthorized`, `Completed` are
terminal.

Execution fields (`nodeName`, `nodeUID`, `podsToDrain`, `faultClass`, `action`,
`groupPolicy`, `deadlineSeconds`) are immutable; `nodeUID`, `selectedPods`, and
`deadlineAt` in status survive spec edits. A DRAIN can never become a PURGE; create a
new request. Status is merge-patched, so earlier fields (for example `pdbBlocked`)
remain visible after the phase moves on.

## Rollout order

1. Choose exactly one node-replacement owner per node group (see ARCHITECTURE.md), and
   set `MIN_READY_NODES_PER_NODE_GROUP` for every node group that serves traffic, with a
   node group maximum that leaves room for replacement ahead of removal.
2. Deploy `deploy/aws/*.yaml` with real rules `DISABLED`; note the `QueueUrl` outputs.
3. Build, scan, and push both images to the customer registry; pin by digest.
4. Apply CRD, RBAC (controller, adapter, approver, one `workload-namespace-rbac.yaml`
   per allowed namespace), then the Deployments with placeholders replaced.
5. Install NTH with `deploy/helm/nth-values.yaml` (chart 0.27.6 / app 1.25.6 validated)
   into the same namespace as the Pod Identity association; tag instances
   `aws-node-termination-handler/managed=true`.
6. Run the adapter with `OBSERVE_ONLY=true` until real dark-launch envelopes look right.
7. Enable one event code, set `OBSERVE_ONLY=false`, validate on staging node groups.
8. Optional: pre-retirement drain. Set `SCHEDULED_CHANGE_ACTION=drain` on the adapter and
   `ScheduledChangeRuleState=ENABLED` on the issue stack; inject a synthetic retirement
   naming several nodes of one group (`make_issue_event.py --scheduled-in-minutes`) and
   confirm they drain one at a time behind the budget and floor.
9. Enable PURGE last: bind approver RBAC, `ENABLE_PURGE=true`, `optional/purge-rbac.yaml`.
10. Enable real EventBridge rules.

Kill switch: disable the EventBridge rules, then scale the adapter and controller to
zero. Never auto-uncordon; the health condition may still be real.

## Validation

`python -m pytest tests -q` runs offline. `docs/VALIDATION.md` records the live
results: controller and adapter semantics (Run A), the production shape with Cluster
Autoscaler on EKS managed node groups, upstream NTH, full upstream NVSentinel driving
this controller from an injected GPU fault, and Cluster Autoscaler replacing and
reclaiming nodes after every drain (Run B), and the 0.3.0 node-group budget, capacity
floor, pre-retirement drain, EBS volume hand-off, and coexistence with EKS node auto
repair (Run C).
`validation/fixtures/` generates synthetic events for the `SyntheticRuleState=ENABLED`
rules; `make_nth_sqs_message.py` output must be sent directly to the NTH queue because
NTH ignores non-`aws.health` sources.

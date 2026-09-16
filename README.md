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
| `integrations/nvsentinel/` | Drain template + node-drainer values for NVSentinel custom-drain mode. |
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
`Purging` -> `Completed` (PURGE). `Blocked` means another non-terminal request owns
the Node. `NodeNotFound`, `TimedOut`, `PurgeNotAuthorized`, `Completed` are terminal.

Execution fields (`nodeName`, `nodeUID`, `podsToDrain`, `faultClass`, `action`,
`groupPolicy`, `deadlineSeconds`) are immutable; `nodeUID`, `selectedPods`, and
`deadlineAt` in status survive spec edits. A DRAIN can never become a PURGE; create a
new request. Status is merge-patched, so earlier fields (for example `pdbBlocked`)
remain visible after the phase moves on.

## Rollout order

1. Choose exactly one node-replacement owner per node group (see ARCHITECTURE.md).
2. Deploy `deploy/aws/*.yaml` with real rules `DISABLED`; note the `QueueUrl` outputs.
3. Build, scan, and push both images to the customer registry; pin by digest.
4. Apply CRD, RBAC (controller, adapter, approver, one `workload-namespace-rbac.yaml`
   per allowed namespace), then the Deployments with placeholders replaced.
5. Install NTH with `deploy/helm/nth-values.yaml` (chart 0.27.6 / app 1.25.6 validated)
   into the same namespace as the Pod Identity association; tag instances
   `aws-node-termination-handler/managed=true`.
6. Run the adapter with `OBSERVE_ONLY=true` until real dark-launch envelopes look right.
7. Enable one event code, set `OBSERVE_ONLY=false`, validate on staging node groups.
8. Enable PURGE last: bind approver RBAC, `ENABLE_PURGE=true`, `optional/purge-rbac.yaml`.
9. Enable real EventBridge rules.

Kill switch: disable the EventBridge rules, then scale the adapter and controller to
zero. Never auto-uncordon; the health condition may still be real.

## Validation

`python -m pytest tests -q` runs offline. `docs/VALIDATION.md` records the live
results for version 0.2.1. `validation/fixtures/` generates synthetic events for the
`SyntheticRuleState=ENABLED` rules; `make_nth_sqs_message.py` output must be sent
directly to the NTH queue because NTH ignores non-`aws.health` sources.

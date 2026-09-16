# Architecture and ownership

## Why two entry paths

AWS Health reports two different things about an EC2 instance. A `scheduledChange`
(retirement, maintenance reboot) has a known deadline and the running computation is
still trustworthy; the right answer is to stop new scheduling and let work finish.
An `issue` (degraded instance store, hardware fault) means the current computation
may already be unsafe; the right answer depends on the fault class and on whether the
workload is a single pod or a distributed gang. Upstream AWS Node Termination Handler
(NTH) implements the first case and explicitly skips `issue`-category events
(`pkg/monitor/sqsevent/scheduled-change-event.go`), so the second case needs custom
code. Forking NTH would couple fault policy to an upstream lifecycle component.

## Signal paths

| Path | Chain | Owner of the mutation |
|---|---|---|
| Planned change | AWS Health `scheduledChange` -> EventBridge -> KMS-encrypted SQS -> NTH Queue Processor, `cordonOnly=true`, `checkTagBeforeDraining=true`, `useProviderId=true` | NTH cordons the Node and stops. No eviction, no deadline. |
| Active fault | AWS Health `issue` -> EventBridge -> KMS-encrypted SQS -> `aws-health-issue-adapter` -> `DrainRequest` -> `node-remediation-controller` | Controller cordons and applies PRESERVE / DRAIN / PURGE. |
| GPU-local fault (optional) | NVSentinel detects, classifies, quarantines -> node-drainer custom-drain renders `DrainRequest` -> same controller | Controller drains `spec.podsToDrain`, sets `DrainComplete=True`; node-drainer deletes the request. |

The two AWS paths use separate queues, keys, rules, and IAM roles so a
misconfiguration in one cannot feed the other.

## Adapter policy

The adapter validates the envelope (`account`, `region`, `source`, `detail-type`,
`service=EC2`, `eventTypeCategory=issue`, `eventScopeCode=ACCOUNT_SPECIFIC`,
`statusCode=open`, and `detail.affectedAccount` / `detail.eventRegion` when present),
extracts instance IDs from `affectedEntities[].entityValue` and `resources`, caps the
count, and maps each ID to exactly one Node by `spec.providerID`.

- An event code must be listed in `ENABLED_DRAIN_EVENT_CODES` to act at all. Codes
  with a classification drain; enabled-but-unclassified codes PRESERVE. Codes that are
  not enabled create nothing by default (`UNKNOWN_EVENT_CODE_ACTION=ignore`).
- Instances that are not a managed Node in this cluster are skipped and the message
  is acknowledged (`UNMAPPED_INSTANCE_POLICY=skip`). An account-wide EC2 issue event
  routinely lists instances that are not cluster nodes; treating them as poison would
  fill the dead-letter queue and, worse, block remediation of the nodes that are
  ours. `fail` is available for clusters that want the stricter behavior.
- Two Nodes with the same providerID is always an error (message retries, then DLQ).
- The DrainRequest name is derived from `detail.eventArn` + instance ID, so updates
  to the same Health event, `affectedEntities` pagination, and duplicate SQS delivery
  all resolve to the same object. Adapter-created requests are `approved: true`,
  `allowForceAfterDeadline: false`, `groupPolicy: NodeLocal`; the adapter cannot
  express PURGE.

## Controller policy

The controller owns only workload policy. It has no EC2, Auto Scaling, managed node
group, Cluster Autoscaler, Karpenter, Node-delete, or NodeClaim permissions, and its
baseline ClusterRole cannot evict or delete anything: eviction is a per-namespace
Role (`workload-namespace-rbac.yaml`) and delete is a separate optional per-namespace
Role (`optional/purge-rbac.yaml`), so the software namespace allowlist and RBAC agree.

Safety controls, all live-validated (see VALIDATION.md):

- **Node fence.** `spec.nodeUID` or, when absent (NVSentinel), the UID read once and
  persisted in status (`NodeLocked`) before any mutation. The cordon is a JSON Patch
  that `test`s the UID and the managed label before `add`ing `unschedulable`.
- **Managed-node allowlist.** Only Nodes whose `MANAGED_NODE_LABEL_KEY` carries one of
  `MANAGED_NODE_LABEL_VALUES`. Unmanaged targets fail closed with phase `Error`.
- **Durable selection.** Target pod UIDs and the deadline are persisted
  (`TargetsLocked`) before the first Eviction. Replacement pods (new UIDs) are never
  adopted, including JobSet replacements that carry the same group-id annotation.
- **Sticky execution state.** `nodeUID`, `selectedPods`, `deadlineAt` survive spec
  edits. The CRD makes `nodeName`, `nodeUID`, `podsToDrain`, `faultClass`, `action`,
  `groupPolicy`, `deadlineSeconds` immutable; only `approved` and
  `allowForceAfterDeadline` can change, and the latter only on a PURGE request.
- **Pre-mutation revalidation.** Before every Eviction or delete the controller
  re-reads the DrainRequest (same UID, same generation, still approved) and the Node
  (same UID, still managed). Any mismatch aborts the batch.
- **PDB-aware drain.** `policy/v1 Eviction` with a UID precondition. 429 is reported
  as `DrainBlocked`; the PDB is never bypassed. After `deadlineSeconds` a DRAIN ends
  `TimedOut`. There is no automatic DRAIN -> PURGE escalation.
- **Gated purge.** Force deletion (`gracePeriodSeconds: 0`, UID precondition) requires
  all of: `faultClass` Fatal/Integrity, `action: PURGE`, `approved: true`,
  `allowForceAfterDeadline: true`, deadline expired, `ENABLE_PURGE=true`, and the
  namespace delete Role. The result reports `fencingVerified: false`; deleting a Pod
  object is not physical fencing.
- **One owner per Node.** The oldest non-terminal DrainRequest for a Node acts;
  others report `Blocked` until it is terminal or deleted. Escalating from PRESERVE to
  DRAIN is therefore an explicit, audited delete-and-create.
- **Leader election.** A `coordination.k8s.io` Lease in the controller namespace;
  standby replicas stay passive and take over within the lease duration (immediately
  on graceful shutdown).
- **Selection scope.** Non-terminal, non-mirror, non-DaemonSet pods in
  `ALLOWED_NAMESPACES`. `NodeLocal` selects pods on the Node; `Gang` requires one
  explicit `nvsentinel.nvidia.com/group-id` on every local pod and selects that group
  cluster-wide; `podsToDrain` (NVSentinel) selects exactly the listed pods.

## Workload lifecycle

Ordinary Deployments and StatefulSets recreate evicted pods. Distributed jobs need a
job-level controller: JobSet with `podFailurePolicy` mapping `DisruptionTarget` to
`FailJob` and `failurePolicy` `RestartJobSet` (validated), or Volcano `RestartJob`
(documented alternative, not validated here). Force deletion does not set
`DisruptionTarget`; the Job then fails via `backoffLimit` and JobSet still restarts
under its default rule. Kubernetes preserves no model, optimizer, or data-loader
state. Without a durable application checkpoint, restart means restart from zero.

## Node replacement without Karpenter

The controller leaves the Node cordoned and empty. "Selected pods are gone" is not
"replacement capacity exists". Choose exactly one owner per node group and validate
it in customer staging:

- **Cluster Autoscaler** is demand-driven. It adds capacity when pods are Pending
  and removes nodes it considers unneeded; it does not guarantee one-for-one
  replacement or removal of the specific cordoned instance. Test, with the exact CA
  version and flags: manually cordoned empty-node scale-down, `safe-to-evict=false`,
  PDBs, node-group discovery, scale-from-zero, labels/taints, GPU capacity, AZ
  topology, and minimum sizes.
- **EKS managed node group repair** acts on supported node-monitoring conditions.
  Where it owns a condition, do not run another actuator for the same condition.
- **Fixed-capacity ASG/MNG actuator** replaces the exact instance after
  `DrainComplete` while preserving desired capacity. It must be scoped to the one
  instance and must not race CA or managed repair over desired capacity.

Scheduled retirement is different again: NTH `cordonOnly` does not evict, so at the
retirement time AWS stops the instance and the remaining pods die non-gracefully.
The ASG or MNG then replaces the instance on its own health check. If that is not
acceptable, the customer needs an explicit pre-retirement drain workflow.

GPU scale-out additionally depends on instance availability, quotas, labels, taints,
device plugin resources, topology, drivers, and scheduler constraints.

## Validation boundary

The lab used Karpenter only to provision and reclaim disposable nodes. That evidence
says nothing about Cluster Autoscaler or managed-node-group behavior. Full NVSentinel
was not installed; real AWS Health envelopes were not captured. See VALIDATION.md.

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

## Which AWS Health signals are acted on

The complete EC2 event-type catalog was reviewed against what production EKS and GPU
fleets inside AWS actually automate. Enumerate the current catalog yourself with
`aws health describe-event-types --filter services=EC2` (Business or Enterprise
Support). The result is deliberately narrow.

| Signal | Category | Path | Rationale |
|---|---|---|---|
| Instance retirement (`*_RETIREMENT_SCHEDULED`, `*_RETIREMENT_EXPEDITED`), stop, termination, reboot maintenance (`*_REBOOT_*_MAINTENANCE_SCHEDULED`) | `scheduledChange` | NTH, cordon-only; optionally also adapter, budgeted DRAIN (`SCHEDULED_CHANGE_ACTION=drain`) | The host has degraded memory, storage, network, or power and EC2 has scheduled its replacement. This is how serious host hardware degradation reaches a customer. NTH matches by category, so new codes are covered automatically. The optional drain path exists because retirement campaigns routinely schedule several instances of one node group for the same minute; see "Correlated retirements" below. |
| Network and power maintenance that keeps the instance running (`*_NETWORK_MAINTENANCE_SCHEDULED`, `*_POWER_MAINTENANCE_SCHEDULED`) | `scheduledChange` | NTH, cordon-only | Non-destructive, but NTH still cordons because it acts by category. Acceptable: a cordon costs nothing and the ASG/MNG owner may treat it as a rotate signal. |
| `AWS_EC2_INSTANCE_STORE_DRIVE_PERFORMANCE_DEGRADED` | `issue` | Adapter, DRAIN | The only per-instance `issue` code that describes live hardware degradation on a still-running instance. Local NVMe is failing under running work; drain now. The one `issue` code every internal fleet automates. |
| `INSTANCE_AVAILABILITY_ISSUE`, `INSTANCE_UNAVAILABLE`, `INSTANCE_AUTO_RECOVERY_FAILURE`, `SIMPLIFIED_AUTO_RECOVERY_FAILURE`, `INSTANCE_POWER_MAINTENANCE_FAILED` | `issue` / `accountNotification` | Not acted on | The instance is already down. Kubernetes marks the node NotReady, EC2 auto-recovery or the ASG/MNG health check replaces it, and draining a dead node achieves nothing. No internal fleet automates these. |
| `INSTANCE_CONSTRAINED_BANDWIDTH_ISSUE` | `issue` | Not acted on | Degraded, not failed; a capacity condition, not a hardware fault. |
| UltraServer, capacity-block, dedicated-host, Local Zone, AZ network health, API, Spot, ODCR, RI, BYOIP codes | various | Not acted on | Not a per-instance hardware fault, or the affected entity is not an EC2 instance the cluster can map. |
| GPU or accelerator degradation | none exists | NVSentinel / EKS node monitoring | AWS Health has no GPU-degradation code. Every internal GPU fleet detects GPU faults in-cluster (DCGM, XID, node problem detection), which is why the NVSentinel path exists. |

Codes that are not enabled create nothing; the adapter logs `event_ignored` with the
code so a dark launch shows exactly which signals arrive before anything is enabled.

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
- **Node-group budget.** Before a request is admitted (its Node fence persisted), the
  controller counts requests in flight (`NodeLocked` through `Purging`) for nodes in
  the same node group (`NODE_GROUP_LABEL_KEY`, default the EKS managed-node-group
  label; fallback: the managed label value) and, optionally, the Ready schedulable
  nodes the group would keep. Over budget or under the floor, the request waits in
  `Blocked` with `blockedReason` `NodeGroupBudget` or `NodeGroupCapacity` and is
  re-evaluated every poll, oldest first. Once admitted a request is never re-gated: a
  cordoned node must not be abandoned half-drained because a sibling appeared.
  `Preserving` requests do not hold budget; their cordoned node is counted by the
  floor instead.
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
  replacement or removal of the specific cordoned instance. Validated here with CA
  v1.36.1 on managed node groups (VALIDATION.md Run B): a drained node's Pending
  replacement triggers scale-up of the same node group, and a cordoned node that the
  drain emptied is removed as `ScaleDownEmpty` once the unneeded timer expires,
  including nodes NVSentinel cordoned. A node whose drain ended `TimedOut` (PDB)
  stays cordoned and occupied, by design. Re-test with your exact CA flags:
  `safe-to-evict=false` annotations, expanders, minimum sizes, AZ topology, and the
  scale-down timers, which set how long a drained node lingers.
- **EKS managed node group repair** acts on supported node-monitoring conditions.
  Where it owns a condition, do not run another actuator for the same condition.
- **Fixed-capacity ASG/MNG actuator** replaces the exact instance after
  `DrainComplete` while preserving desired capacity. It must be scoped to the one
  instance and must not race CA or managed repair over desired capacity.

Scheduled retirement is different again: NTH `cordonOnly` does not evict, so at the
retirement time AWS stops the instance and the remaining pods die non-gracefully.
The ASG or MNG then replaces the instance on its own health check. The section below
is the pre-retirement drain workflow for customers who do not accept that.

## Correlated retirements and the node-group budget

EC2 retires hardware in campaigns. A rack retirement can schedule several instances
of one node group for the same start time, and outside AWS there is no throttle on how
many of your instances stop in one window. Inside AWS this exact pattern has caused
customer-facing outages: an internal fleet lost every instance of a service in one
Availability Zone when four were stopped within five minutes, and the resulting
review concluded that any actuator must (a) limit how many nodes of a group are
being remediated at once and (b) refuse to act when the group would drop below a
healthy-capacity floor. Both are implemented here and were reproduced live
(VALIDATION.md Run C):

- With the budget alone (`MAX_CONCURRENT_PER_NODE_GROUP=1`,
  `MIN_READY_NODES_PER_NODE_GROUP=0`) three simultaneous retirement drains ran
  strictly one at a time, but each finished in about ten seconds because the pods
  evicted cleanly, so all three nodes were cordoned within 30 s. Cluster Autoscaler hit
  the node group's maximum size and the workload sat Pending for about six minutes
  while it reclaimed the empty nodes and scaled back up. Serialization is not
  capacity protection.
- With the floor (`MIN_READY_NODES_PER_NODE_GROUP=2`) the second and third drains were
  admitted only after Cluster Autoscaler had delivered a replacement node, so the
  group never fell below two schedulable nodes and the pace of draining became the
  pace of replacement.

Set `MIN_READY_NODES_PER_NODE_GROUP` for every node group that serves traffic. Size the
node group's maximum at least `min ready + in-flight budget + 1` above its normal size
or Cluster Autoscaler cannot replace ahead of removal.

Two ways to feed retirements into that budget:

- **Adapter path (validated).** `SCHEDULED_CHANGE_ACTION=drain` plus the stack's
  `ScheduledChangeRuleState=ENABLED` deliver `scheduledChange` events to the adapter,
  which creates one `Recoverable/DRAIN` request per node with a deadline that ends
  `SCHEDULED_CHANGE_MARGIN_SECONDS` before `detail.startTime`. NTH keeps cordoning
  from its own queue; the cordon is idempotent and the two never evict the same pod.
  A drain a PDB refuses ends `TimedOut` and is visible days before EC2 acts.
- **NTH drain mode (rejected).** `cordonOnly: false` with `nodeTerminationGracePeriod`
  was tested live: in queue-processor mode NTH drained the node one second after the
  event arrived, ignoring an 8-minute `startTime`, with no per-group limit. That is an
  immediate unbudgeted drain of every affected node, which is the failure mode above.

GPU scale-out additionally depends on instance availability, quotas, labels, taints,
device plugin resources, topology, drivers, and scheduler constraints.

## Validation boundary

Controller and adapter semantics were first validated on Karpenter-provisioned CPU
nodes (VALIDATION.md Run A). The production shape was then validated end to end
(Run B): Cluster Autoscaler owning EKS managed node groups, upstream NTH, full upstream
NVSentinel in custom-drain mode driving this controller from a real injected GPU
fault, and Cluster Autoscaler replacing and reclaiming nodes after every drain,
including a Gang drain of a two-node GPU JobSet. Real AWS Health envelopes were not
captured and remain the dark-launch gate. See VALIDATION.md.

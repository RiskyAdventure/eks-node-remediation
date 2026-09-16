# Live validation record

Two live runs, both on 2026-09-16, both on an isolated, non-production Amazon EKS
cluster (Kubernetes v1.36, us-west-2), both using images built from this repository
with the packaged Dockerfiles and deployed from the packaged manifests (placeholders
rendered, nothing else changed). Offline suite: `python -m pytest tests -q` passed
(66 tests) against the same source.

- **Run A** (package 0.2.1/0.2.3): controller and adapter semantics on CPU nodes that
  Karpenter provisioned. Karpenter is not part of the solution; Run A says nothing
  about node replacement.
- **Run B** (package 0.2.3): the production shape. Cluster Autoscaler owning two EKS
  managed node groups (one CPU, one GPU), upstream NTH, full upstream NVSentinel in
  custom-drain mode, and a two-GPU JobSet. Karpenter was fenced off from these node
  groups (it refused every pod pinned to them) so no result below is contaminated by it.

## Run B: Cluster Autoscaler + managed node groups + NVSentinel + GPU

Component versions, all pinned: Cluster Autoscaler v1.36.1 (chart 9.59.0,
autodiscovery by tag, 2 m unneeded/scale-down timers, EKS Pod Identity); managed node
groups on `AL2023_x86_64_NVIDIA` (GPU, landed on g5.xlarge / NVIDIA A10G) and
`AL2023_x86_64_STANDARD` (CPU, c6a.large), both min 0; NVIDIA device plugin v0.20.0;
standalone DCGM 4.6.0 hostengine DaemonSet; NVSentinel v1.22.0 (operator-service DCGM
mode, fault-quarantine, node-drainer `customDrain` pointing at this CRD, MongoDB store
on gp2 via aws-ebs-csi-driver v1.66.0); NTH 1.25.6 (chart 0.27.6); JobSet v0.12.0.
Both node groups carried the managed label values and the
`aws-node-termination-handler/managed=true` instance tag from the launch template.

| # | Scenario | Observed | Result |
|---|---|---|---|
| B1 | Real GPU fault through the whole NVSentinel chain. `dcgmi test --inject -f 230 -v 95` (XID-class fatal) on the A10G. | gpu-health-monitor raised the event; fault-quarantine cordoned the Node and labeled it `cordon-reason=GPU-fatal-error-ruleset`; node-drainer rendered `drain-<node>-<eventID>` in `nvsentinel` from the packaged template; controller `TargetsLocked` 6 s later, evicted the CUDA pod, `Completed` with `DrainComplete=True` 46 s after injection; node-drainer logged "Drain CR completed" and deleted the DrainRequest. No hand edits anywhere in the chain. | Pass |
| B2 | Cluster Autoscaler replaces after a drain. | The evicted pod went Pending; CA scaled the GPU node group 1 -> 2 and the pod ran on the new node. CA then declared the NVSentinel-cordoned empty node unneeded and removed it (`ScaleDownEmpty`) after the 2 m timer. Scale-from-zero of the CPU node group worked from the discovery tags alone. | Pass |
| B3 | NTH on a CA-owned managed node. Canonical `aws.health` scheduledChange body on the NTH queue for a CPU node. | NTH matched the instance by provider ID and tag, cordoned it, and stopped (cordonOnly). Canary pod untouched. | Pass |
| B4 | DRAIN of that node against a `minAvailable: 1` PDB. | `DrainBlocked` with `pdbBlocked` UID, then `TimedOut` after the deadline; the unprotected canary was evicted and CA added a second CPU node for it. PDB honored, no bypass. | Pass |
| B5 | PURGE with all gates satisfied on the same node. | `Purging` force-deleted exactly the persisted pod UID, `fencingVerified=false`, `Completed`; replacement scheduled on the new CPU node. CA later removed the empty cordoned node. | Pass |
| B6 | AWS Health `issue` for a GPU node through EventBridge -> KMS SQS -> adapter, code `AWS_EC2_INSTANCE_STORE_DRIVE_PERFORMANCE_DEGRADED`, two-pod JobSet spread across two GPU nodes. | Adapter created one `Fatal/DRAIN` NodeLocal request; the JobSet leader on that node was evicted; JobSet `restarts` 0 -> 1 with new Job and Pod UIDs; CA scaled the GPU group for the replacement and then removed the drained node (`Scale-down: removing empty node`). | Pass |
| B7 | Gang DRAIN on a JobSet GPU node (`groupPolicy: Gang`, `Fatal`). | Both pods (two nodes) were selected by UID and evicted; `DrainComplete=True/SelectedPodsGone`; JobSet `restarts` 1 -> 2, new Job and Pod UIDs; CA added a third GPU node for the Pending leader and both replacements ran within about 3 minutes. | Pass |

Observations recorded for customers, not defects in this package:

- NVSentinel's node state label stayed at `draining` after the drain completed. In
  v1.22.0 the transition to `drain-succeeded` and the uncordon belong to the
  fault-remediation module, which was not enabled. Expect this if you run
  quarantine + drain without remediation.
- Cluster Autoscaler does not remove a cordoned node until it is empty *and* unneeded
  for the configured timer. With a PDB-blocked `TimedOut` drain the node stays
  cordoned and occupied until an operator decides. That is the intended safety
  behavior, not a gap.
- EKS Pod Identity credentials held by an already-running Cluster Autoscaler pod went
  stale when its IAM association was recreated; a `rollout restart` fixed it. Recreate
  associations before installing CA, or restart it afterward.
- `helm install --wait` for NVSentinel timed out (release status `failed`) because the
  GPU DaemonSets could not become ready before the first GPU node finished booting.
  Every component came up on its own afterward; a `helm upgrade` clears the status.

## Run A: controller and adapter semantics (CPU nodes)

| # | Scenario | Observed | Result |
|---|---|---|---|
| 1 | PRESERVE without `spec.nodeUID` (NVSentinel-style request) | Phase `NodeLocked` persisted the Node UID, then `Preserving`; Node `spec.unschedulable=true`; canary pod same UID, `Running`, 0 restarts; Event `Cordoned`. | Pass |
| 2a | Second request for the same Node | `Blocked: Node ... is owned by DrainRequest node-remediation-system/s1-preserve until it is terminal`. | Pass |
| 2b | CRD immutability | `nodeName`, `action` edits rejected by CEL; `allowForceAfterDeadline=true` on a DRAIN rejected. | Pass |
| 2c | Owner deleted, DRAIN takes over | `TargetsLocked` -> `Evicting` (Eviction body carried the exact pod UID precondition) -> `Completed` with `DrainComplete=True/SelectedPodsGone`; Deployment recreated the pod elsewhere. | Pass |
| 3 | DRAIN against a `minAvailable: 1` PDB | `DrainBlocked` with `pdbBlocked` UID; after `deadlineSeconds` -> `TimedOut: Drain deadline expired; PDB and grace were not bypassed`; pod same UID, `Running`. | Pass |
| 4 | Approval revoked and re-granted mid-drain | gen 2 `AwaitingApproval`, gen 3 `DrainBlocked`; `selectedPods`, `deadlineAt`, `nodeUID` identical across all three generations. | Pass |
| 5a | PURGE with `ENABLE_PURGE=false` | Eviction attempted first (PDB blocked); after deadline `PurgeNotAuthorized`; `kubectl auth can-i delete pods` = no; pod untouched. | Pass |
| 5b | PURGE with flag + namespace delete Role | `Purging` force-deleted exactly the persisted UID (`gracePeriodSeconds: 0`, UID precondition), `fencingVerified=false`, then `Completed`; delete still `no` in other namespaces. | Pass |
| 6 | Gang DRAIN of a 2-pod JobSet across 2 nodes | Both original pod UIDs selected and evicted; JobSet `status.restarts=1`; child Job UIDs and Pod UIDs changed; replacements not adopted; `DrainComplete=True`. JobSet chart 0.12.0. | Pass |
| 7a | Synthetic AWS Health issue event (EventBridge -> KMS SQS -> adapter): code not enabled | `event_ignored` (UNKNOWN_EVENT_CODE_ACTION=ignore), no DrainRequest. | Pass |
| 7b | Instance not in cluster | `node_not_found`, message acknowledged (queue and DLQ depth 0), no DrainRequest. | Pass |
| 7c | Mixed event: one managed node + one foreign instance, enabled code | Exactly one `Fatal/DRAIN` DrainRequest for the managed node; foreign instance skipped. | Pass |
| 7d | Same event delivered twice | `drain_request_exists`; still one DrainRequest (name derived from `eventArn` + instance). | Pass |
| 7e | `OBSERVE_ONLY=true` (package default) | `event_observed` with `would_create` name; no DrainRequest. | Pass |
| 8 | NTH 1.25.6 (chart 0.27.6, EKS Pod Identity), canonical `aws.health` scheduledChange body on the NTH queue | NTH resolved the instance by provider ID (`checkTagBeforeDraining` satisfied by the `aws-node-termination-handler/managed=true` tag), cordoned the Node, and stopped (cordonOnly); canary pod `Running`, 0 restarts. | Pass |
| 7f | Account-specific issue event with no instance IDs (0.2.3, re-tested live through EventBridge) | `event_ignored: no EC2 instance IDs in event`, message acknowledged, queue and DLQ depth 0. Previously this was retried into the DLQ. | Pass |
| 9 | Leader failover (2 controller replicas) | Lease held by one replica; after deleting the leader the lease moved (SIGTERM release) and a new leader logged `leading` in under 13 s. Before the release logic was added, failover took ~60 s (termination grace + lease duration). | Pass |

## Not validated here (customer staging gates)

- Real AWS Health envelopes. Synthetic events mirror the documented schema (bare
  instance IDs, `eventArn`, `eventRegion`, `affectedAccount`, `page/totalPages`) but a
  real event must be captured in dark launch before enabling the real rules.
- Organizational AWS Health delivery (`affectedAccount` != receiving account).
- EKS managed node group repair and a fixed-capacity ASG actuator as the replacement
  owner. Only Cluster Autoscaler was exercised.
- NVSentinel fault-remediation (the module that moves `draining` to
  `drain-succeeded` and uncordons). Only quarantine and node-drainer were enabled.
- Your Cluster Autoscaler flags. Run B used a 2 m scale-down timer to keep the lab
  fast; production timers, expanders, and priorities change the timing, not the
  sequence.

# Live validation record

Package version 0.2.3 (policy and adapter behavior identical to the 0.2.1 run except where noted). Validated 2026-09-16 on an isolated, non-production Amazon EKS
cluster (Kubernetes v1.36, us-west-2) using images built from this repository with
the packaged Dockerfiles and deployed from the packaged manifests (placeholders
rendered, nothing else changed). Karpenter provisioned the disposable c6a.large test
nodes; it is **not** part of the solution and none of the results below say anything
about Cluster Autoscaler, managed-node-group, or Auto Scaling group behavior.

Offline suite: `python -m pytest tests -q` passed (66 tests) against the same source.

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
- Full NVSentinel (detector -> quarantine -> node-drainer custom drain -> this plugin).
  The plugin side was validated with template-shaped requests only.
- Node replacement by Cluster Autoscaler, EKS managed node group repair, or an ASG
  actuator. The lab had none of these.
- Organizational AWS Health delivery (`affectedAccount` != receiving account).
- GPU instances. All scenarios ran on CPU nodes; the controller logic is identical,
  but GPU scheduling constraints were not exercised.

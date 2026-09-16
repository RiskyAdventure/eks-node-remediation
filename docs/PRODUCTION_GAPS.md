# Production readiness

Version 0.2.1 is a live-validated reference implementation. The items below separate
what is now implemented from what a customer must still add or decide.

## Implemented in 0.2.x

- Leader election (Lease) with graceful hand-over; one active reconciler.
- One owner per Node (`Blocked` phase) so concurrent requests cannot interleave.
- Execution-field immutability in the CRD (CEL) and sticky fence/selection/deadline
  in status across spec edits; no DRAIN -> PURGE escalation path.
- Per-namespace eviction and delete RBAC matching the software allowlist; no
  cluster-wide evict or delete.
- Configurable managed-node label key/values with fail-closed startup; no lab defaults.
- Kubernetes Events and a `DrainComplete` condition on the DrainRequest.
- Service-account token re-read per request (bound token rotation).
- Stable adapter idempotency (`eventArn` + instance) across Health updates,
  pagination, and duplicate delivery; 409 treated as existing.
- Explicit unknown-event-code policy (default: no action) and unmapped-instance
  policy (default: skip and acknowledge); ambiguous mappings always fail.
- `detail.affectedAccount` / `detail.eventRegion` validation when present.
- KMS key policy scoped with `aws:SourceAccount`; DLQ and stalled-queue alarms;
  EventBridge target retry policy; synthetic rules parameterized and disabled by default.
- Deployments: non-root, read-only root filesystem, seccomp, dropped capabilities,
  protected-tier placement placeholders, anti-affinity.

## Required before production

Controller and adapter:

- Watches/informers instead of 5 s polling and full pod lists. At current scale the
  poll is cheap; on large clusters the `GET /api/v1/pods` for Gang selection is not.
- Prometheus metrics (phase counts, evictions, PDB blocks, purge count, lease state,
  queue lag). Today only structured logs and Events exist.
- Admission policy (ValidatingAdmissionPolicy) that limits `approved: true` on PURGE
  requests to the approver group and prevents the adapter identity from creating
  anything but NodeLocal DRAIN/PRESERVE. RBAC alone cannot express this.
- SQS visibility extension for slow processing and explicit backoff; today a message
  that exceeds 60 s is redelivered.
- Dead-letter replay runbook and tooling.
- Fleet, AZ, and node-group concurrent-remediation budgets (how many nodes may be
  cordoned or draining at once). The per-event cap is the only limit today.
- Organizational AWS Health delivery: the adapter is single-account. If a management
  account receives events for member accounts, `account` != `affectedAccount` and the
  envelope check must be redesigned.
- Digest-pinned images, SBOM, scanning, and signature verification in the customer
  pipeline. The Dockerfiles pin the base image by tag only.
- Status field hygiene: status is merge-patched, so transient fields persist after
  the phase changes; consider clearing them on transition.
- A no-auto-uncordon workflow: who decides a Node is healthy again, and how.

Customer decisions:

- Exactly one node-replacement owner per node group (Cluster Autoscaler, EKS managed
  node group repair, or a scoped fixed-capacity ASG/MNG actuator), validated in
  staging with the exact versions and flags.
- An independent infrastructure-fencing signal before treating an Integrity purge as
  complete; the controller reports `fencingVerified: false` on purpose.
- Real AWS Health envelopes captured in dark launch before enabling real rules.
- If NVSentinel is used: pinned NVSentinel, cert-manager, GPU Operator/device plugin,
  DCGM, and the two files in `integrations/nvsentinel/` validated end to end.
- Application checkpointing for distributed jobs; Kubernetes restarts from zero.

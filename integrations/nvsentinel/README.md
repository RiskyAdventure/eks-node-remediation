# NVSentinel integration boundary

This repository does not fork or copy NVSentinel. The remediation controller is an
independent implementation of NVIDIA's documented **drain plugin** contract
(NVSentinel v1.22.0, "Writing a Drain Plugin"):

- The plugin defines and owns the CRD `nvsentinel.nvidia.com/v1alpha1 DrainRequest`.
  NVSentinel does not ship this CRD; there is no CRD collision as long as exactly one
  drain plugin is installed per cluster.
- node-drainer renders a Go template (`drain-template.example.yaml`) into one
  DrainRequest per health event, named `drain-{node}-{eventID}` in the `nvsentinel`
  namespace, and polls `status.conditions` for `DrainComplete=True`.
- Once the condition is set node-drainer marks the drain complete and **deletes** the
  DrainRequest. The controller must therefore be idempotent and must not depend on
  the object surviving.
- `customDrain` and `userNamespaces` are mutually exclusive; with
  `node-drainer-values.example.yaml` NVSentinel never evicts pods itself, so the
  native drainer and this plugin cannot act on the same fault.

## What the controller adds on top of the contract

The contract only requires draining `spec.podsToDrain` and setting the condition.
This controller additionally: persists the Node UID before mutation (NVSentinel
does not template `nodeUID`), cordons the Node with a UID-tested JSON Patch,
persists the selected Pod UIDs, evicts through `policy/v1 Eviction` (PDB-aware),
honors `deadlineSeconds`, never escalates to force deletion, and reports
`DrainComplete=True` only when every selected Pod UID is gone.

## Operational requirements

- `ALLOWED_NAMESPACES` on the controller must include every namespace NVSentinel
  may list in `podsToDrain`; pods in other namespaces are silently excluded and the
  request will never complete. Grant `workload-namespace-rbac.yaml` for each.
- `MANAGED_NODE_LABEL_VALUES` must include the label value of every GPU node group
  NVSentinel may quarantine, otherwise the request fails closed (`Error`).
- The controller's RBAC must allow `list` on DrainRequests cluster-wide (it does)
  because NVSentinel creates them in `nvsentinel`, not in the controller namespace.
- Set `customDrain.timeout` above `deadlineSeconds + terminationGracePeriodSeconds`.
  If the deadline expires first the request ends `TimedOut` and node-drainer
  eventually times out; NVSentinel's timeout handling is version-specific.
- A PDB-blocked drain surfaces as `DrainBlocked` and, after the deadline, `TimedOut`.
  The controller will not bypass the PDB; an operator must decide.

## Validation status

Full NVSentinel (platform-connectors, fault-quarantine, node-drainer,
gpu-health-monitor) was **not** installed in the lab. The controller's plugin
behavior was validated standalone by applying DrainRequests shaped like the
template output (`spec.podsToDrain`, no `nodeUID`). Customer staging must pin
NVSentinel, cert-manager, GPU Operator/device plugin, and DCGM versions, apply
these two example files, inject a fault, and observe the node state label move
`quarantined -> draining -> drain-succeeded` end to end.

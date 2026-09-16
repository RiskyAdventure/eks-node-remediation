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
- `customDrain.timeout` is parsed with `strconv.Atoi`: write integer seconds as a
  string (`"1800"`), not a Go duration (`"30m"` fails to load).

## Installing NVSentinel on EKS without the GPU Operator

These are the prerequisites that were missing on a stock EKS cluster with NVIDIA
AMIs (host drivers, `nvidia` containerd runtime, device plugin) and no GPU Operator:

- A DCGM host engine. NVSentinel's gpu-health-monitor connects to DCGM over TCP;
  `dcgm-hostengine.example.yaml` runs one per GPU node and exposes it as
  `nvidia-dcgm.nvidia-dcgm.svc:5555` with `internalTrafficPolicy: Local`. Set
  `global.dcgm.mode: operator-service` and point `global.dcgm.service` at it.
- A RuntimeClass named `nvidia` (`runtimeclass-nvidia.example.yaml`); the
  metadata-collector DaemonSet requires it.
- `labeler.assumeDriverInstalled: true` so the labeler does not wait for a GPU
  Operator driver label that will never appear.
- A default StorageClass backed by a CSI driver (the aws-ebs-csi-driver add-on) for
  the MongoDB store's PersistentVolumeClaims.
- Schedule the GPU-node DaemonSets with `global.nodeSelector` and
  `global.tolerations` for your GPU taints. The chart's MongoDB setup Job does not
  accept tolerations, so the control-plane-style components (MongoDB, fault-quarantine,
  node-drainer) must land on an untainted general-purpose tier.
- `helm install --wait` may time out on the first install while GPU nodes are still
  booting; the components converge on their own and a later `helm upgrade` clears the
  release status.

## Validation status

Validated end to end with NVSentinel v1.22.0 (platform-connectors, gpu-health-monitor
in DCGM 4.x mode, labeler, metadata-collector, MongoDB store, fault-quarantine,
node-drainer with the values above and the packaged drain template) on an EKS
managed node group of NVIDIA A10G instances owned by Cluster Autoscaler. An injected
DCGM fatal error (`dcgmi test --inject -f 230 -v 95`) produced: quarantine cordon with
`k8saas.nvidia.com/cordon-reason=GPU-fatal-error-ruleset`, a `drain-<node>-<eventID>`
DrainRequest rendered by node-drainer, controller drain and `DrainComplete=True` 46 s
after injection, node-drainer "Drain CR completed" and deletion of the request, then
Cluster Autoscaler removing the empty cordoned node. See `docs/VALIDATION.md` Run B.

Not enabled: fault-remediation. Without it the node state label stops at `draining`
(the `drain-succeeded` transition and uncordon belong to that module in v1.22.0).
Customers still pin their own NVSentinel, device plugin, and DCGM versions and rerun
the injection in staging.

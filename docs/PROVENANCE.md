# Code provenance

| Component | Origin | Status in this solution |
|---|---|---|
| AWS Node Termination Handler | Upstream AWS (`aws/aws-node-termination-handler`) | Unmodified. Helm chart 0.27.6 / app 1.25.6 via `deploy/helm/nth-values.yaml`. |
| NVIDIA NVSentinel | Upstream NVIDIA | Optional. Unmodified, not forked, not installed by this package. Only its documented drain-plugin contract is implemented. |
| JobSet | Upstream Kubernetes SIGs | Unmodified. Chart 0.12.0 used in validation. |
| Volcano | Upstream CNCF | Unmodified. Documented alternative; not validated here. |
| `aws-health-issue-adapter` | Custom | Written from scratch for this solution. |
| `node-remediation-controller` | Custom | Written from scratch. Implements NVIDIA's documented custom-drain plugin contract (NVSentinel v1.22.0, "Writing a Drain Plugin"): the plugin defines the `nvsentinel.nvidia.com/v1alpha1 DrainRequest` CRD, drains `spec.podsToDrain`, and sets `status.conditions[DrainComplete]=True`. |
| DrainRequest CRD | Custom | Defined by this package as the contract requires. `nodeName` and `podsToDrain` follow the documented shape; all other fields are this solution's policy extensions. |
| CloudFormation, RBAC, Deployments, Dockerfiles, fixtures | Custom | Written for this solution. |

No NVIDIA source code was copied or adapted. The custom code was written from the
public behavior description only: cordon, PDB-aware Eviction, custom DrainRequest
completion condition, and separately gated force deletion. The runtime is substantial
enough that it must be maintained in a versioned internal repository and released as
immutable, scanned images in the customer's registry; it is not a set of ConfigMap
scripts.

Base image: `public.ecr.aws/docker/library/python:3.13-slim-bookworm`; pin by digest
in the customer build pipeline. Adapter dependency: `boto3==1.43.95`.

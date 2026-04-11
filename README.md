# CarbonOps

CarbonOps is an early Rust project exploring FinOps plus carbon attribution for Kubernetes and cloud infrastructure.

The first goal is to test feasibility in a local homelab Kubernetes cluster before expanding toward AKS and broader Azure resources.

## Project Direction

The intended direction is to build a Rust backend that can:

- collect Kubernetes pod, node, namespace, and workload metadata
- collect CPU and memory usage
- model cost attribution for homelab infrastructure
- model carbon attribution with explicit assumptions
- roll results up by namespace, workload, application, and team
- later extend the model to AKS and Azure-native resources

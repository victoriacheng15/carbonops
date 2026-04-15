# CarbonOps

CarbonOps is an experimental Rust CLI for collecting Kubernetes infrastructure data and shaping it into early FinOps plus carbon attribution signals.

The project is focused on learning how local and cloud Kubernetes environments differ in practice: kubeconfig access, Kubernetes API usage, metrics availability, energy telemetry, and workload attribution. It starts with local homelab Kubernetes and extends the same collector flow to AKS.

Cost, energy, and carbon values are estimates unless measured telemetry is available from tools such as Kepler.

## Milestones

- Validated local Kubernetes with Prometheus/Kepler and AKS without Prometheus/Kepler, proving CarbonOps can use measured energy telemetry when available and fall back to an estimated CPU-based model when needed.

Visual gallery: [`docs/visual-gallery`](docs/visual-gallery)

## Capabilities

CarbonOps currently supports:

- detecting whether Prometheus and Kepler are available in the current cluster
- querying Kepler power metrics through Prometheus when available
- falling back to estimated node energy usage when measured telemetry is unavailable
- reading Kubernetes node, pod, namespace, and placement data
- reading current CPU and memory usage from the Kubernetes Metrics API
- calculating estimated kWh, CAD cost, and carbon impact per hour
- showing the telemetry source used for each impact row

## Requirements

CarbonOps uses the current Kubernetes context from kubeconfig.

For basic collection:

- access to a Kubernetes cluster
- `metrics-server` or another provider for the Kubernetes Metrics API
- RBAC permissions to read pods, nodes, services, and pod metrics

For measured energy attribution:

- Prometheus
- Kepler
- access to query Kepler metrics through Prometheus

When Prometheus or Kepler is not available, CarbonOps still runs with the estimated CPU model.

## Usage

Run telemetry detection against the current Kubernetes context:

```bash
cargo run -- detect
```

Collect current metrics and impact estimates:

```bash
cargo run -- collect
```

Limit collection to one namespace:

```bash
cargo run -- collect --namespace kube-system
```

Adjust local assumptions:

```bash
cargo run -- collect \
  --node-idle-watts 50 \
  --node-max-watts 180 \
  --electricity-cad-per-kwh 0.20 \
  --carbon-gco2e-per-kwh 400
```

## AKS Testing

For AKS testing, use a separate kubeconfig file so the project does not modify the default `~/.kube/config`:

```bash
cd tofu
tofu init
tofu plan
tofu apply

az aks get-credentials \
  --resource-group rg-carbonops-aks \
  --name aks-carbonops \
  --file ./kubeconfig-aks-carbonops
```

Then run CarbonOps with that kubeconfig:

```bash
KUBECONFIG=./tofu/kubeconfig-aks-carbonops cargo run -- detect
KUBECONFIG=./tofu/kubeconfig-aks-carbonops cargo run -- collect --namespace kube-system
```

## Roadmap

Near-term:

- require namespace-scoped collection by default for safer cloud testing
- add explicit `--all-namespaces` for full-cluster collection
- add top-N views for the highest cost, energy, carbon, CPU, or memory contributors
- add JSON output for future dashboard and automation use
- continue validating against small AKS clusters

Longer-term:

- store historical snapshots
- calculate baseline usage by namespace, workload, node, or team
- compare current usage against baseline history
- flag cost, energy, or carbon anomalies
- send alerts through tools such as Grafana, Slack, Teams, or GitHub issues
- package the collector as a Kubernetes workload through Helm

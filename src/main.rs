use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use k8s_openapi::api::core::v1::{Node, Pod, Service};
use kube::{
    Client,
    api::{Api, DynamicObject, ListParams},
    core::{ApiResource, GroupVersionKind},
};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Parser)]
#[command(name = "carbonops")]
#[command(about = "Collect Kubernetes usage data for FinOps and carbon attribution experiments")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Detect available energy telemetry in the current Kubernetes cluster.
    Detect,

    /// Collect current Kubernetes Metrics API usage.
    Collect {
        /// Limit pod inventory and pod metrics to one namespace.
        #[arg(short, long)]
        namespace: Option<String>,

        /// Estimated idle power draw per node in watts.
        #[arg(long, default_value_t = 50.0)]
        node_idle_watts: f64,

        /// Estimated max power draw per node in watts.
        #[arg(long, default_value_t = 180.0)]
        node_max_watts: f64,

        /// Electricity price in CAD per kWh.
        #[arg(long, default_value_t = 0.20)]
        electricity_cad_per_kwh: f64,

        /// Carbon intensity in grams CO2e per kWh.
        #[arg(long, default_value_t = 400.0)]
        carbon_gco2e_per_kwh: f64,

        /// Output format for the collection snapshot.
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        output: OutputFormat,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    Table,
    Json,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct EstimateConfig {
    node_idle_watts: f64,
    node_max_watts: f64,
    electricity_cad_per_kwh: f64,
    carbon_gco2e_per_kwh: f64,
}

#[derive(Debug, Clone, Serialize)]
struct ContainerMetric {
    node: String,
    namespace: String,
    pod: String,
    container: String,
    #[serde(rename = "cpu_mcores")]
    cpu_millicores: f64,
    memory_mib: f64,
}

#[derive(Debug)]
struct TelemetryDetection {
    prometheus_found: bool,
    kepler_found: bool,
    kepler_metrics_queryable: bool,
    prometheus_target: Option<String>,
    energy_source: EnergySource,
}

#[derive(Debug, Clone, Copy)]
enum EnergySource {
    Estimated,
    MeasuredKepler,
}

#[derive(Debug, Serialize)]
struct CollectionReport {
    collected_at_unix_seconds: u64,
    telemetry: TelemetryReport,
    telemetry_notes: Vec<TelemetryNote>,
    assumptions: EstimateConfig,
    node_impact_per_hour: Vec<ImpactRow>,
    workload_metrics: Vec<ContainerMetric>,
}

#[derive(Debug, Serialize)]
struct TelemetryReport {
    prometheus: bool,
    kepler: bool,
    kepler_queryable: bool,
    prometheus_target: Option<String>,
    energy_source: String,
}

#[derive(Debug, Serialize)]
struct TelemetryNote {
    level: String,
    message: String,
}

#[derive(Debug, Serialize)]
struct ImpactRow {
    energy_source: String,
    node: String,
    cpu_mcores: Option<f64>,
    alloc_mcores: Option<f64>,
    cpu_pct: Option<f64>,
    watts: f64,
    kwh_per_hour: f64,
    cad_per_hour: f64,
    gco2e_g_per_hour: f64,
}

impl EnergySource {
    fn as_str(self) -> &'static str {
        match self {
            EnergySource::Estimated => "estimated_cpu_model",
            EnergySource::MeasuredKepler => "measured_kepler",
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Detect => {
            let client = Client::try_default()
                .await
                .context("connect to Kubernetes using the current kubeconfig")?;
            let detection = detect_telemetry(client).await?;
            print_detection(&detection);
            print_telemetry_notes(&detection, None);
        }
        Commands::Collect {
            namespace,
            node_idle_watts,
            node_max_watts,
            electricity_cad_per_kwh,
            carbon_gco2e_per_kwh,
            output,
        } => {
            let config = EstimateConfig {
                node_idle_watts,
                node_max_watts,
                electricity_cad_per_kwh,
                carbon_gco2e_per_kwh,
            };

            collect(namespace.as_deref(), config, output).await?
        }
    }

    Ok(())
}

async fn collect(
    namespace: Option<&str>,
    config: EstimateConfig,
    output: OutputFormat,
) -> Result<()> {
    let client = Client::try_default()
        .await
        .context("connect to Kubernetes using the current kubeconfig")?;
    let detection = detect_telemetry(client.clone()).await?;
    let measured_node_watts = if matches!(detection.energy_source, EnergySource::MeasuredKepler) {
        match detection.prometheus_target.as_deref() {
            Some(target) => query_kepler_node_watts(client.clone(), target)
                .await
                .unwrap_or_default(),
            None => HashMap::new(),
        }
    } else {
        HashMap::new()
    };
    let node_allocatable = collect_node_allocatable_cpu(client.clone()).await?;
    let pod_nodes = collect_pod_nodes(client.clone(), namespace).await?;
    let metrics = collect_pod_metrics(client, namespace, &pod_nodes).await?;
    let report = build_collection_report(
        detection,
        config,
        node_allocatable,
        measured_node_watts,
        metrics,
    )?;

    match output {
        OutputFormat::Table => print_collection_report(&report),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .context("serialize collection report as JSON")?
            );
        }
    }

    Ok(())
}

async fn detect_telemetry(client: Client) -> Result<TelemetryDetection> {
    let services: Api<Service> = Api::all(client.clone());
    let pods: Api<Pod> = Api::all(client.clone());
    let service_list = services
        .list(&ListParams::default())
        .await
        .context("list Kubernetes services while detecting Prometheus and Kepler")?;
    let pod_list = pods
        .list(&ListParams::default())
        .await
        .context("list Kubernetes pods while detecting Prometheus and Kepler")?;

    let mut prometheus_services: Vec<_> = service_list
        .iter()
        .filter(|service| is_prometheus_query_service(object_name(&service.metadata.name)))
        .collect();
    prometheus_services.sort_by_key(|service| {
        prometheus_service_score(
            object_name(&service.metadata.name),
            service.spec.as_ref().and_then(|spec| spec.ports.as_ref()),
        )
    });

    let prometheus_found = !prometheus_services.is_empty()
        || pod_list.iter().any(|pod| {
            object_name(&pod.metadata.name)
                .to_ascii_lowercase()
                .contains("prometheus")
        });

    let kepler_found = service_list.iter().any(|service| {
        object_name(&service.metadata.name)
            .to_ascii_lowercase()
            .contains("kepler")
    }) || pod_list.iter().any(|pod| {
        object_name(&pod.metadata.name)
            .to_ascii_lowercase()
            .contains("kepler")
    });

    let mut prometheus_target = None;
    let mut kepler_metrics_queryable = false;

    for service in prometheus_services {
        if let Some(target) = prometheus_service_proxy_target(service) {
            let queryable = query_prometheus_for_kepler(client.clone(), &target)
                .await
                .unwrap_or(false);

            prometheus_target = Some(target);
            kepler_metrics_queryable = queryable;

            if queryable {
                break;
            }
        }
    }

    let energy_source = if prometheus_found && kepler_found && kepler_metrics_queryable {
        EnergySource::MeasuredKepler
    } else {
        EnergySource::Estimated
    };

    Ok(TelemetryDetection {
        prometheus_found,
        kepler_found,
        kepler_metrics_queryable,
        prometheus_target,
        energy_source,
    })
}

fn prometheus_service_proxy_target(service: &Service) -> Option<String> {
    let namespace = object_namespace(&service.metadata.namespace);
    let name = object_name(&service.metadata.name);
    let port = service
        .spec
        .as_ref()
        .and_then(|spec| spec.ports.as_ref())
        .and_then(|ports| {
            ports
                .iter()
                .find(|port| port.port == 9090)
                .or_else(|| ports.first())
        })?;

    Some(format!("{namespace}/{name}:{}", port.port))
}

fn is_prometheus_query_service(name: &str) -> bool {
    let name = name.to_ascii_lowercase();

    name.contains("prometheus")
        && !name.contains("thanos")
        && !name.contains("kube-state-metrics")
        && !name.contains("node-exporter")
}

fn prometheus_service_score(
    name: &str,
    ports: Option<&Vec<k8s_openapi::api::core::v1::ServicePort>>,
) -> u8 {
    let name = name.to_ascii_lowercase();

    if name == "prometheus" || name == "prometheus-server" {
        return 0;
    }

    if has_service_port(ports, 9090) {
        return 1;
    }

    if has_service_port(ports, 80) {
        return 2;
    }

    3
}

fn has_service_port(
    ports: Option<&Vec<k8s_openapi::api::core::v1::ServicePort>>,
    wanted_port: i32,
) -> bool {
    ports.is_some_and(|ports| ports.iter().any(|port| port.port == wanted_port))
}

async fn query_prometheus_for_kepler(client: Client, service_target: &str) -> Result<bool> {
    let (namespace, service) = service_target
        .split_once('/')
        .context("Prometheus service target must be namespace/service:port")?;
    let metric_names = [
        "kepler_container_joules_total",
        "kepler_container_cpu_joules_total",
        "kepler_container_core_joules_total",
        "kepler_container_dram_joules_total",
        "kepler_node_cpu_joules_total",
        "kepler_node_package_joules_total",
    ];

    for metric_name in metric_names {
        let path = format!(
            "/api/v1/namespaces/{namespace}/services/{service}/proxy/api/v1/query?query={metric_name}"
        );
        let request = http::Request::get(path)
            .body(Vec::new())
            .context("build Prometheus proxy request")?;
        let response = client.request_text(request).await?;
        let payload: Value =
            serde_json::from_str(&response).context("parse Prometheus response")?;
        let has_results = payload
            .get("data")
            .and_then(|data| data.get("result"))
            .and_then(Value::as_array)
            .is_some_and(|result| !result.is_empty());

        if has_results {
            return Ok(true);
        }
    }

    Ok(false)
}

async fn query_kepler_node_watts(
    client: Client,
    service_target: &str,
) -> Result<HashMap<String, f64>> {
    let query = "sum by (node_name) (rate(kepler_node_cpu_joules_total{zone=\"package\"}[5m]))";
    let payload = query_prometheus(client, service_target, query).await?;
    let mut node_watts = HashMap::new();

    if let Some(results) = prometheus_results(&payload) {
        for result in results {
            let node = result
                .get("metric")
                .and_then(|metric| metric.get("node_name"))
                .and_then(Value::as_str);
            let watts = result
                .get("value")
                .and_then(Value::as_array)
                .and_then(|value| value.get(1))
                .and_then(Value::as_str)
                .and_then(|value| value.parse::<f64>().ok());

            if let (Some(node), Some(watts)) = (node, watts) {
                node_watts.insert(node.to_string(), watts);
            }
        }
    }

    Ok(node_watts)
}

async fn query_prometheus(client: Client, service_target: &str, query: &str) -> Result<Value> {
    let (namespace, service) = service_target
        .split_once('/')
        .context("Prometheus service target must be namespace/service:port")?;
    let path = format!(
        "/api/v1/namespaces/{namespace}/services/{service}/proxy/api/v1/query?query={}",
        url_encode(query)
    );
    let request = http::Request::get(path)
        .body(Vec::new())
        .context("build Prometheus proxy request")?;
    let response = client.request_text(request).await?;
    serde_json::from_str(&response).context("parse Prometheus response")
}

fn prometheus_results(payload: &Value) -> Option<&Vec<Value>> {
    payload
        .get("data")
        .and_then(|data| data.get("result"))
        .and_then(Value::as_array)
}

fn url_encode(value: &str) -> String {
    let mut encoded = String::new();

    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }

    encoded
}

fn print_detection(detection: &TelemetryDetection) {
    let rows = vec![vec![
        detection.prometheus_found.to_string(),
        detection.kepler_found.to_string(),
        detection.kepler_metrics_queryable.to_string(),
        detection
            .prometheus_target
            .as_deref()
            .unwrap_or("<none>")
            .to_string(),
        detection.energy_source.as_str().to_string(),
    ]];

    print_table(
        "carbonops telemetry detection",
        &[
            "prometheus",
            "kepler",
            "kepler_queryable",
            "prometheus_target",
            "energy_source",
        ],
        &rows,
    );
}

fn print_telemetry_notes(
    detection: &TelemetryDetection,
    measured_node_watts: Option<&HashMap<String, f64>>,
) {
    let notes = telemetry_notes(detection, measured_node_watts);
    print_telemetry_notes_table(&notes);
}

fn telemetry_notes(
    detection: &TelemetryDetection,
    measured_node_watts: Option<&HashMap<String, f64>>,
) -> Vec<TelemetryNote> {
    let mut notes = Vec::new();

    if !detection.prometheus_found {
        notes.push(TelemetryNote {
            level: "warning".to_string(),
            message: "Prometheus was not found. CarbonOps will use estimated_cpu_model energy."
                .to_string(),
        });
    } else if detection.prometheus_target.is_none() {
        notes.push(TelemetryNote {
            level: "warning".to_string(),
            message: "Prometheus was found, but no queryable service target was selected."
                .to_string(),
        });
    }

    if !detection.kepler_found {
        notes.push(TelemetryNote {
            level: "warning".to_string(),
            message: "Kepler was not found. CarbonOps cannot use measured node energy.".to_string(),
        });
    }

    if detection.prometheus_found && detection.kepler_found && !detection.kepler_metrics_queryable {
        notes.push(TelemetryNote {
            level: "warning".to_string(),
            message: "Prometheus and Kepler were found, but Kepler metrics were not queryable."
                .to_string(),
        });
    }

    if matches!(detection.energy_source, EnergySource::Estimated) {
        notes.push(TelemetryNote {
            level: "info".to_string(),
            message: "Measured energy is unavailable. Impact rows will use estimated_cpu_model."
                .to_string(),
        });
    }

    if matches!(detection.energy_source, EnergySource::MeasuredKepler)
        && measured_node_watts.is_some_and(HashMap::is_empty)
    {
        notes.push(TelemetryNote {
            level: "warning".to_string(),
            message: "Kepler was queryable, but no node watts were returned. Node rows will fall back to estimated_cpu_model.".to_string(),
        });
    }

    notes
}

fn print_telemetry_notes_table(notes: &[TelemetryNote]) {
    if notes.is_empty() {
        return;
    }

    let rows: Vec<Vec<String>> = notes
        .iter()
        .map(|note| vec![note.level.clone(), note.message.clone()])
        .collect();

    print_table("\ncarbonops telemetry notes", &["level", "message"], &rows);
}

async fn collect_node_allocatable_cpu(client: Client) -> Result<HashMap<String, f64>> {
    let nodes: Api<Node> = Api::all(client);
    let mut node_allocatable = HashMap::new();

    for node in nodes
        .list(&ListParams::default())
        .await
        .context("list Kubernetes nodes for allocatable CPU")?
    {
        let name = object_name(&node.metadata.name);
        let cpu = node
            .status
            .as_ref()
            .and_then(|status| status.allocatable.as_ref())
            .and_then(|allocatable| allocatable.get("cpu"))
            .and_then(|quantity| parse_cpu_millicores(&quantity.0));

        if let Some(cpu) = cpu {
            node_allocatable.insert(name.to_string(), cpu);
        }
    }

    Ok(node_allocatable)
}

async fn collect_pod_nodes(
    client: Client,
    namespace: Option<&str>,
) -> Result<HashMap<String, String>> {
    let pods: Api<Pod> = match namespace {
        Some(namespace) => Api::namespaced(client, namespace),
        None => Api::all(client),
    };

    let mut pod_nodes = HashMap::new();
    for pod in pods
        .list(&ListParams::default())
        .await
        .context("list Kubernetes pods for pod-to-node placement")?
    {
        let namespace = object_namespace(&pod.metadata.namespace);
        let name = object_name(&pod.metadata.name);
        let node = pod
            .spec
            .as_ref()
            .and_then(|spec| spec.node_name.as_deref())
            .unwrap_or("<pending>");

        pod_nodes.insert(pod_key(namespace, name), node.to_string());
    }

    Ok(pod_nodes)
}

async fn collect_pod_metrics(
    client: Client,
    namespace: Option<&str>,
    pod_nodes: &HashMap<String, String>,
) -> Result<Vec<ContainerMetric>> {
    let resource = metrics_resource("PodMetrics");
    let api: Api<DynamicObject> = match namespace {
        Some(namespace) => Api::namespaced_with(client, namespace, &resource),
        None => Api::all_with(client, &resource),
    };

    let mut metrics = Vec::new();

    for metric in api
        .list(&ListParams::default())
        .await
        .context("read Kubernetes Metrics API pod metrics; verify metrics-server is installed and RBAC allows pod metrics")?
    {
        let metric_namespace = object_namespace(&metric.metadata.namespace).to_string();
        let pod = object_name(&metric.metadata.name).to_string();
        let node = pod_nodes
            .get(&pod_key(&metric_namespace, &pod))
            .map(String::as_str)
            .unwrap_or("<unknown>");

        if let Some(containers) = metric.data.get("containers").and_then(Value::as_array) {
            for container in containers {
                let name = container
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("<unknown>");
                let usage = container.get("usage").unwrap_or(&Value::Null);

                metrics.push(ContainerMetric {
                    node: node.to_string(),
                    namespace: metric_namespace.clone(),
                    pod: pod.clone(),
                    container: name.to_string(),
                    cpu_millicores: parse_cpu_millicores(metric_value(usage, "cpu"))
                        .unwrap_or_default(),
                    memory_mib: parse_memory_mib(metric_value(usage, "memory")).unwrap_or_default(),
                });
            }
        }
    }

    Ok(metrics)
}

fn build_collection_report(
    detection: TelemetryDetection,
    config: EstimateConfig,
    node_allocatable: HashMap<String, f64>,
    measured_node_watts: HashMap<String, f64>,
    metrics: Vec<ContainerMetric>,
) -> Result<CollectionReport> {
    let collected_at_unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("calculate collection timestamp")?
        .as_secs();
    let telemetry_notes = telemetry_notes(&detection, Some(&measured_node_watts));
    let node_impact_per_hour =
        build_impact_rows(&metrics, &node_allocatable, &measured_node_watts, config);

    Ok(CollectionReport {
        collected_at_unix_seconds,
        telemetry: TelemetryReport {
            prometheus: detection.prometheus_found,
            kepler: detection.kepler_found,
            kepler_queryable: detection.kepler_metrics_queryable,
            prometheus_target: detection.prometheus_target,
            energy_source: detection.energy_source.as_str().to_string(),
        },
        telemetry_notes,
        assumptions: config,
        node_impact_per_hour,
        workload_metrics: metrics,
    })
}

fn build_impact_rows(
    metrics: &[ContainerMetric],
    node_allocatable: &HashMap<String, f64>,
    measured_node_watts: &HashMap<String, f64>,
    config: EstimateConfig,
) -> Vec<ImpactRow> {
    let mut cpu_by_node: HashMap<&str, f64> = HashMap::new();
    for metric in metrics {
        *cpu_by_node.entry(&metric.node).or_default() += metric.cpu_millicores;
    }

    let mut rows = Vec::new();
    let mut total_watts = 0.0;
    let mut total_kwh_per_hour = 0.0;
    let mut total_cad_per_hour = 0.0;
    let mut total_gco2e_per_hour = 0.0;
    let mut measured_rows = 0;
    let mut estimated_rows = 0;

    let mut node_usage: Vec<_> = cpu_by_node.into_iter().collect();
    node_usage.sort_by_key(|(node, _)| *node);

    for (node, cpu_millicores) in node_usage {
        let allocatable_millicores = node_allocatable.get(node).copied().unwrap_or_default();
        let cpu_utilization = if allocatable_millicores > 0.0 {
            (cpu_millicores / allocatable_millicores).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let (energy_source, watts) = match measured_node_watts.get(node).copied() {
            Some(watts) => {
                measured_rows += 1;
                (EnergySource::MeasuredKepler, watts)
            }
            None => {
                estimated_rows += 1;
                (
                    EnergySource::Estimated,
                    config.node_idle_watts
                        + ((config.node_max_watts - config.node_idle_watts) * cpu_utilization),
                )
            }
        };
        let kwh_per_hour = watts / 1000.0;
        let cad_per_hour = kwh_per_hour * config.electricity_cad_per_kwh;
        let gco2e_per_hour = kwh_per_hour * config.carbon_gco2e_per_kwh;

        total_watts += watts;
        total_kwh_per_hour += kwh_per_hour;
        total_cad_per_hour += cad_per_hour;
        total_gco2e_per_hour += gco2e_per_hour;

        rows.push(ImpactRow {
            energy_source: energy_source.as_str().to_string(),
            node: node.to_string(),
            cpu_mcores: Some(cpu_millicores),
            alloc_mcores: Some(allocatable_millicores),
            cpu_pct: Some(cpu_utilization * 100.0),
            watts,
            kwh_per_hour,
            cad_per_hour,
            gco2e_g_per_hour: gco2e_per_hour,
        });
    }

    let total_energy_source = match (measured_rows > 0, estimated_rows > 0) {
        (true, true) => "mixed",
        (true, false) => EnergySource::MeasuredKepler.as_str(),
        _ => EnergySource::Estimated.as_str(),
    };

    rows.push(ImpactRow {
        energy_source: total_energy_source.to_string(),
        node: "total".to_string(),
        cpu_mcores: None,
        alloc_mcores: None,
        cpu_pct: None,
        watts: total_watts,
        kwh_per_hour: total_kwh_per_hour,
        cad_per_hour: total_cad_per_hour,
        gco2e_g_per_hour: total_gco2e_per_hour,
    });

    rows
}

fn print_collection_report(report: &CollectionReport) {
    print_telemetry_summary_report(&report.telemetry);
    print_telemetry_notes_table(&report.telemetry_notes);
    print_impact_per_hour(&report.node_impact_per_hour);
    print_current_metrics(&report.workload_metrics);
}

fn print_telemetry_summary_report(telemetry: &TelemetryReport) {
    let rows = vec![vec![
        telemetry.prometheus.to_string(),
        telemetry.kepler.to_string(),
        telemetry.kepler_queryable.to_string(),
        telemetry.energy_source.clone(),
    ]];

    print_table(
        "carbonops telemetry",
        &["prometheus", "kepler", "kepler_queryable", "energy_mode"],
        &rows,
    );
}

fn print_impact_per_hour(impact_rows: &[ImpactRow]) {
    let rows: Vec<Vec<String>> = impact_rows
        .iter()
        .map(|row| {
            vec![
                row.energy_source.clone(),
                row.node.clone(),
                row.cpu_mcores
                    .map(|value| format!("{value:.2}"))
                    .unwrap_or_default(),
                row.alloc_mcores
                    .map(|value| format!("{value:.2}"))
                    .unwrap_or_default(),
                row.cpu_pct
                    .map(|value| format!("{value:.2}"))
                    .unwrap_or_default(),
                format!("{:.2}", row.watts),
                format!("{:.4}", row.kwh_per_hour),
                format!("{:.4}", row.cad_per_hour),
                format!("{:.2}", row.gco2e_g_per_hour),
            ]
        })
        .collect();

    print_table(
        "\ncarbonops impact per hour",
        &[
            "energy_source",
            "node",
            "cpu_mcores",
            "alloc_mcores",
            "cpu_pct",
            "watts",
            "kwh_per_hr",
            "cad_per_hr",
            "gco2e_g_per_hr",
        ],
        &rows,
    );
}

fn print_current_metrics(metrics: &[ContainerMetric]) {
    let rows: Vec<Vec<String>> = metrics
        .iter()
        .map(|metric| {
            vec![
                metric.node.clone(),
                metric.namespace.clone(),
                metric.pod.clone(),
                metric.container.clone(),
                format!("{:.2}", metric.cpu_millicores),
                format!("{:.2}", metric.memory_mib),
            ]
        })
        .collect();

    print_table(
        "\ncarbonops current metrics",
        &[
            "node",
            "namespace",
            "pod",
            "container",
            "cpu_mcores",
            "memory_mib",
        ],
        &rows,
    );
}

fn metrics_resource(kind: &str) -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", kind))
}

fn object_name(name: &Option<String>) -> &str {
    name.as_deref().unwrap_or("<unknown>")
}

fn object_namespace(namespace: &Option<String>) -> &str {
    namespace.as_deref().unwrap_or("default")
}

fn pod_key(namespace: &str, pod: &str) -> String {
    format!("{namespace}/{pod}")
}

fn metric_value<'a>(usage: &'a Value, key: &str) -> &'a str {
    usage
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("<missing>")
}

fn parse_cpu_millicores(quantity: &str) -> Option<f64> {
    if let Some(value) = quantity.strip_suffix('n') {
        return value
            .parse::<f64>()
            .ok()
            .map(|nanocores| nanocores / 1_000_000.0);
    }

    if let Some(value) = quantity.strip_suffix('u') {
        return value
            .parse::<f64>()
            .ok()
            .map(|microcores| microcores / 1_000.0);
    }

    if let Some(value) = quantity.strip_suffix('m') {
        return value.parse::<f64>().ok();
    }

    quantity.parse::<f64>().ok().map(|cores| cores * 1_000.0)
}

fn parse_memory_mib(quantity: &str) -> Option<f64> {
    let units = [
        ("Ki", 1.0 / 1024.0),
        ("Mi", 1.0),
        ("Gi", 1024.0),
        ("Ti", 1024.0 * 1024.0),
        ("K", 1000.0 / 1024.0 / 1024.0),
        ("M", 1000.0 * 1000.0 / 1024.0 / 1024.0),
        ("G", 1000.0 * 1000.0 * 1000.0 / 1024.0 / 1024.0),
        ("T", 1000.0 * 1000.0 * 1000.0 * 1000.0 / 1024.0 / 1024.0),
    ];

    for (suffix, multiplier) in units {
        if let Some(value) = quantity.strip_suffix(suffix) {
            return value.parse::<f64>().ok().map(|amount| amount * multiplier);
        }
    }

    quantity
        .parse::<f64>()
        .ok()
        .map(|bytes| bytes / 1024.0 / 1024.0)
}

fn print_table(title: &str, headers: &[&str], rows: &[Vec<String>]) {
    println!("{title}:");

    if rows.is_empty() {
        println!("(none)");
        return;
    }

    let mut widths: Vec<usize> = headers.iter().map(|header| header.len()).collect();
    for row in rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(value.len());
        }
    }

    print_table_row(headers, &widths);
    print_table_separator(&widths);
    for row in rows {
        let values: Vec<&str> = row.iter().map(String::as_str).collect();
        print_table_row(&values, &widths);
    }
}

fn print_table_row(values: &[&str], widths: &[usize]) {
    for (index, value) in values.iter().enumerate() {
        print!("| {:width$} ", value, width = widths[index]);
    }
    println!("|");
}

fn print_table_separator(widths: &[usize]) {
    for width in widths {
        print!("|-{}-", "-".repeat(*width));
    }
    println!("|");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_close(actual: f64, expected: f64) {
        let delta = (actual - expected).abs();
        assert!(
            delta < 0.0001,
            "expected {actual} to be within 0.0001 of {expected}"
        );
    }

    #[test]
    fn parses_cpu_quantities_as_millicores() {
        assert_close(parse_cpu_millicores("250m").unwrap(), 250.0);
        assert_close(parse_cpu_millicores("2").unwrap(), 2000.0);
        assert_close(parse_cpu_millicores("1500000u").unwrap(), 1500.0);
        assert_close(parse_cpu_millicores("250000000n").unwrap(), 250.0);
        assert!(parse_cpu_millicores("bad").is_none());
    }

    #[test]
    fn parses_memory_quantities_as_mib() {
        assert_close(parse_memory_mib("128Mi").unwrap(), 128.0);
        assert_close(parse_memory_mib("1Gi").unwrap(), 1024.0);
        assert_close(parse_memory_mib("1048576").unwrap(), 1.0);
        assert!(parse_memory_mib("bad").is_none());
    }

    #[test]
    fn telemetry_notes_explain_estimated_fallback() {
        let detection = TelemetryDetection {
            prometheus_found: false,
            kepler_found: false,
            kepler_metrics_queryable: false,
            prometheus_target: None,
            energy_source: EnergySource::Estimated,
        };

        let notes = telemetry_notes(&detection, None);

        assert_eq!(notes.len(), 3);
        assert_eq!(notes[0].level, "warning");
        assert!(notes[0].message.contains("Prometheus was not found"));
        assert!(notes[1].message.contains("Kepler was not found"));
        assert!(notes[2].message.contains("Measured energy is unavailable"));
    }

    #[test]
    fn impact_rows_use_estimated_power_and_total_row() {
        let metrics = vec![ContainerMetric {
            node: "node-a".to_string(),
            namespace: "kube-system".to_string(),
            pod: "coredns".to_string(),
            container: "coredns".to_string(),
            cpu_millicores: 1000.0,
            memory_mib: 64.0,
        }];
        let node_allocatable = HashMap::from([("node-a".to_string(), 2000.0)]);
        let measured_node_watts = HashMap::new();
        let config = EstimateConfig {
            node_idle_watts: 50.0,
            node_max_watts: 180.0,
            electricity_cad_per_kwh: 0.20,
            carbon_gco2e_per_kwh: 400.0,
        };

        let rows = build_impact_rows(&metrics, &node_allocatable, &measured_node_watts, config);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].energy_source, "estimated_cpu_model");
        assert_eq!(rows[0].node, "node-a");
        assert_close(rows[0].cpu_mcores.unwrap(), 1000.0);
        assert_close(rows[0].alloc_mcores.unwrap(), 2000.0);
        assert_close(rows[0].cpu_pct.unwrap(), 50.0);
        assert_close(rows[0].watts, 115.0);
        assert_close(rows[0].kwh_per_hour, 0.115);
        assert_close(rows[0].cad_per_hour, 0.023);
        assert_close(rows[0].gco2e_g_per_hour, 46.0);

        assert_eq!(rows[1].node, "total");
        assert_eq!(rows[1].energy_source, "estimated_cpu_model");
        assert!(rows[1].cpu_mcores.is_none());
        assert_close(rows[1].watts, 115.0);
    }

    #[test]
    fn impact_rows_mark_mixed_totals_when_measured_and_estimated_are_present() {
        let metrics = vec![
            ContainerMetric {
                node: "node-a".to_string(),
                namespace: "default".to_string(),
                pod: "app-a".to_string(),
                container: "app".to_string(),
                cpu_millicores: 500.0,
                memory_mib: 32.0,
            },
            ContainerMetric {
                node: "node-b".to_string(),
                namespace: "default".to_string(),
                pod: "app-b".to_string(),
                container: "app".to_string(),
                cpu_millicores: 500.0,
                memory_mib: 32.0,
            },
        ];
        let node_allocatable = HashMap::from([
            ("node-a".to_string(), 2000.0),
            ("node-b".to_string(), 2000.0),
        ]);
        let measured_node_watts = HashMap::from([("node-b".to_string(), 42.0)]);
        let config = EstimateConfig {
            node_idle_watts: 50.0,
            node_max_watts: 180.0,
            electricity_cad_per_kwh: 0.20,
            carbon_gco2e_per_kwh: 400.0,
        };

        let rows = build_impact_rows(&metrics, &node_allocatable, &measured_node_watts, config);

        assert_eq!(rows[0].node, "node-a");
        assert_eq!(rows[0].energy_source, "estimated_cpu_model");
        assert_eq!(rows[1].node, "node-b");
        assert_eq!(rows[1].energy_source, "measured_kepler");
        assert_eq!(rows[2].node, "total");
        assert_eq!(rows[2].energy_source, "mixed");
    }

    #[test]
    fn json_uses_human_readable_cpu_field_name() {
        let metric = ContainerMetric {
            node: "node-a".to_string(),
            namespace: "default".to_string(),
            pod: "app".to_string(),
            container: "app".to_string(),
            cpu_millicores: 12.5,
            memory_mib: 64.0,
        };

        let value = serde_json::to_value(metric).unwrap();

        assert_eq!(value["cpu_mcores"], 12.5);
        assert!(value.get("cpu_millicores").is_none());
    }
}

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
        estimated_cad_per_hour: 0.0,
        estimated_gco2e_g_per_hour: 0.0,
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
            estimated_cad_per_hour: 0.0,
            estimated_gco2e_g_per_hour: 0.0,
        },
        ContainerMetric {
            node: "node-b".to_string(),
            namespace: "default".to_string(),
            pod: "app-b".to_string(),
            container: "app".to_string(),
            cpu_millicores: 500.0,
            memory_mib: 32.0,
            estimated_cad_per_hour: 0.0,
            estimated_gco2e_g_per_hour: 0.0,
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
        estimated_cad_per_hour: 0.0,
        estimated_gco2e_g_per_hour: 0.0,
    };

    let value = serde_json::to_value(metric).unwrap();

    assert_eq!(value["cpu_mcores"], 12.5);
    assert!(value.get("cpu_millicores").is_none());
}

#[test]
fn parses_estimate_config_from_toml() {
    let config: EstimateConfig = toml::from_str(
        r#"
node_idle_watts = 55.0
node_max_watts = 160.0
electricity_cad_per_kwh = 0.18
carbon_gco2e_per_kwh = 35.0
"#,
    )
    .unwrap();

    assert_close(config.node_idle_watts, 55.0);
    assert_close(config.node_max_watts, 160.0);
    assert_close(config.electricity_cad_per_kwh, 0.18);
    assert_close(config.carbon_gco2e_per_kwh, 35.0);
}

#[test]
fn cli_assumption_overrides_apply_to_default_config() {
    let config = load_estimate_config(
        None,
        EstimateConfigOverrides {
            node_idle_watts: Some(60.0),
            node_max_watts: None,
            electricity_cad_per_kwh: Some(0.25),
            carbon_gco2e_per_kwh: None,
        },
    )
    .unwrap();

    assert_close(config.node_idle_watts, 60.0);
    assert_close(config.node_max_watts, 180.0);
    assert_close(config.electricity_cad_per_kwh, 0.25);
    assert_close(config.carbon_gco2e_per_kwh, 400.0);
}

#[test]
fn namespace_collection_requires_namespace_or_all_namespaces() {
    assert!(resolve_collection_namespace(None, false).is_err());
    assert!(resolve_collection_namespace(Some("payments".to_string()), true).is_err());
    assert_eq!(
        resolve_collection_namespace(Some("payments".to_string()), false).unwrap(),
        Some("payments".to_string())
    );
    assert_eq!(resolve_collection_namespace(None, true).unwrap(), None);
}

#[test]
fn workload_metrics_sort_highest_cpu_first_and_apply_limit() {
    let metrics = vec![
        ContainerMetric {
            node: "node-a".to_string(),
            namespace: "default".to_string(),
            pod: "low".to_string(),
            container: "app".to_string(),
            cpu_millicores: 10.0,
            memory_mib: 32.0,
            estimated_cad_per_hour: 0.001,
            estimated_gco2e_g_per_hour: 2.0,
        },
        ContainerMetric {
            node: "node-a".to_string(),
            namespace: "default".to_string(),
            pod: "high".to_string(),
            container: "app".to_string(),
            cpu_millicores: 90.0,
            memory_mib: 64.0,
            estimated_cad_per_hour: 0.009,
            estimated_gco2e_g_per_hour: 18.0,
        },
        ContainerMetric {
            node: "node-a".to_string(),
            namespace: "default".to_string(),
            pod: "middle".to_string(),
            container: "app".to_string(),
            cpu_millicores: 50.0,
            memory_mib: 48.0,
            estimated_cad_per_hour: 0.005,
            estimated_gco2e_g_per_hour: 10.0,
        },
    ];

    let limited = sort_and_limit_workload_metrics(metrics, TopMetric::Cpu, Some(2));

    assert_eq!(limited.len(), 2);
    assert_eq!(limited[0].pod, "high");
    assert_eq!(limited[1].pod, "middle");
}

#[test]
fn workload_metrics_sort_by_selected_top_dimension() {
    let metrics = vec![
        ContainerMetric {
            node: "node-a".to_string(),
            namespace: "default".to_string(),
            pod: "cpu-heavy".to_string(),
            container: "app".to_string(),
            cpu_millicores: 90.0,
            memory_mib: 32.0,
            estimated_cad_per_hour: 0.002,
            estimated_gco2e_g_per_hour: 4.0,
        },
        ContainerMetric {
            node: "node-a".to_string(),
            namespace: "default".to_string(),
            pod: "memory-heavy".to_string(),
            container: "app".to_string(),
            cpu_millicores: 30.0,
            memory_mib: 128.0,
            estimated_cad_per_hour: 0.008,
            estimated_gco2e_g_per_hour: 16.0,
        },
    ];

    let by_memory = sort_and_limit_workload_metrics(metrics.clone(), TopMetric::Memory, Some(1));
    let by_cost = sort_and_limit_workload_metrics(metrics.clone(), TopMetric::Cost, Some(1));
    let by_carbon = sort_and_limit_workload_metrics(metrics, TopMetric::Carbon, Some(1));

    assert_eq!(by_memory[0].pod, "memory-heavy");
    assert_eq!(by_cost[0].pod, "memory-heavy");
    assert_eq!(by_carbon[0].pod, "memory-heavy");
}

#[test]
fn workload_limit_does_not_change_node_impact_totals() {
    let metrics = vec![
        ContainerMetric {
            node: "node-a".to_string(),
            namespace: "default".to_string(),
            pod: "low".to_string(),
            container: "app".to_string(),
            cpu_millicores: 100.0,
            memory_mib: 32.0,
            estimated_cad_per_hour: 0.0,
            estimated_gco2e_g_per_hour: 0.0,
        },
        ContainerMetric {
            node: "node-a".to_string(),
            namespace: "default".to_string(),
            pod: "high".to_string(),
            container: "app".to_string(),
            cpu_millicores: 900.0,
            memory_mib: 64.0,
            estimated_cad_per_hour: 0.0,
            estimated_gco2e_g_per_hour: 0.0,
        },
    ];
    let detection = TelemetryDetection {
        prometheus_found: false,
        kepler_found: false,
        kepler_metrics_queryable: false,
        prometheus_target: None,
        energy_source: EnergySource::Estimated,
    };
    let node_allocatable = HashMap::from([("node-a".to_string(), 2000.0)]);
    let config = EstimateConfig {
        node_idle_watts: 50.0,
        node_max_watts: 180.0,
        electricity_cad_per_kwh: 0.20,
        carbon_gco2e_per_kwh: 400.0,
    };

    let report = build_collection_report(
        detection,
        config,
        node_allocatable,
        HashMap::new(),
        metrics,
        Some(1),
        TopMetric::Cpu,
    )
    .unwrap();

    assert_eq!(report.workload_metrics.len(), 1);
    assert_eq!(report.workload_metrics[0].pod, "high");
    assert_close(report.node_impact_per_hour[0].cpu_mcores.unwrap(), 1000.0);
}

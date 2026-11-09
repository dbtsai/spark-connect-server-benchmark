use super::*;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "connect-compare-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, AtomicOrdering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn group(scale: f64) -> Value {
    json!({"case": "config/get", "workload": "Config", "session_model": "shared", "channel_model": "shared", "concurrency": 8,
        "requests": 256, "repetitions": 2, "latency_window": "rpc", "throughput_window": "primary", "byte_accounting": "protobuf",
        "rounds": [{"repetition": 0, "requests": 128}, {"repetition": 1, "requests": 128}],
        "median_ms": 2.0/scale, "p95_ms": 3.0/scale, "p99_ms": 4.0/scale,
        "requests_per_s": 100.0*scale, "completed_round_requests_per_s": 80.0*scale, "client_cpu_ns": 25600.0/scale})
}

fn sample(pair: usize, variant: &str, scale: f64) -> Value {
    json!({"pair": pair, "variant": variant, "summary": {"schema_version": 1, "valid": true, "forks": [{
        "target": "rust-jvm-planning", "profile_kind": "planning", "settings": {"endpoint": variant, "requests": 128, "repetitions": 2},
        "groups": [group(scale)], "skipped_cells": [], "non_success_cells": []}]}})
}

fn policy() -> Value {
    json!({"targets": [{"case": "config/get", "concurrency": [8]}], "min_target_throughput_ratio": 1.15,
        "min_improved_targets": 1, "max_median_latency_ratio": 1.2, "min_throughput_ratio": 0.8,
        "max_p95_latency_ratio": 1.5, "max_client_cpu_per_request_ratio": 1.25,
        "min_completed_round_throughput_ratio": 0.8, "require_all_pairs_target_improve": false})
}

fn cells(scale: f64) -> Vec<Value> {
    aggregate(
        &[sample(1, "baseline", 1.0), sample(1, "candidate", scale)],
        1,
    )
    .unwrap()
}

fn options(root: &Path, control: bool) -> CompareOptions {
    let input = root.join("input");
    fs::write(&input, "identical").unwrap();
    let payloads = root.join("source-payloads");
    fs::create_dir(&payloads).unwrap();
    fs::write(
        payloads.join("manifest.json"),
        r#"{"payloads":[],"cases":[]}"#,
    )
    .unwrap();
    let policy_file = root.join("policy.json");
    save(&policy_file, &policy()).unwrap();
    CompareOptions {
        output: root.join("result"),
        harness_binary: input.clone(),
        binary: input.clone(),
        baseline_jar: input.clone(),
        baseline_library: input.clone(),
        candidate_jar: input.clone(),
        candidate_library: input,
        spark_home: root.into(),
        java_home: root.into(),
        payload_dir: Some(payloads),
        policy: Some(policy_file),
        pairs: 2,
        start_order: "AB".into(),
        control,
        profile: "smoke".into(),
        concurrency: vec![1, 8, 32],
        requests: 128,
        warmups: 64,
        repetitions: 2,
        session_models: vec!["shared".into()],
        channel_models: vec!["shared".into()],
        max_prepared_mib: 60,
        client_threads: 4,
        timeout_seconds: 30,
        client_timeout: 3600,
        startup_timeout: 90,
        case: None,
    }
}

#[test]
fn fresh_pair_orders_alternate_and_can_start_ba() {
    assert_eq!(
        (1..=3).map(|i| order(i, "AB")).collect::<Vec<_>>(),
        vec![
            ["baseline", "candidate"],
            ["candidate", "baseline"],
            ["baseline", "candidate"]
        ]
    );
    assert_eq!(order(1, "BA"), ["candidate", "baseline"]);
    assert_eq!(order(2, "BA"), ["baseline", "candidate"]);
}

#[test]
fn ratios_keep_each_fork_pair_and_use_geometric_mean() {
    let cells = aggregate(
        &[
            sample(1, "baseline", 1.0),
            sample(1, "candidate", 2.0),
            sample(2, "baseline", 1.0),
            sample(2, "candidate", 0.5),
        ],
        2,
    )
    .unwrap();
    assert_eq!(cells.len(), 1);
    assert_eq!(
        cells[0]["ratios"]["throughput"],
        json!({"geometric_mean":1.0,"min":0.5,"max":2.0})
    );
    assert_eq!(cells[0]["pairs"][0]["ratios"]["throughput"], 2.0);
    assert_eq!(cells[0]["pairs"][1]["ratios"]["throughput"], 0.5);
    assert_eq!(
        cells[0]["pairs"][0]["baseline"]["client_cpu_per_request"],
        100.0
    );
    assert_eq!(
        cells[0]["ratios"]["client_cpu_per_request"]["geometric_mean"],
        1.0
    );
}

#[test]
fn invalid_fork_identities_and_incomplete_pairs_are_rejected() {
    let mut boolean = sample(1, "baseline", 1.0);
    boolean["pair"] = json!(true);
    let mut fractional = sample(1, "baseline", 1.0);
    fractional["pair"] = json!(1.5);
    for (rows, pairs) in [
        (vec![sample(1, "baseline", 1.0)], 1),
        (
            vec![sample(1, "baseline", 1.0), sample(1, "baseline", 1.0)],
            1,
        ),
        (
            vec![sample(0, "baseline", 1.0), sample(0, "candidate", 1.0)],
            1,
        ),
        (vec![boolean, sample(1, "candidate", 1.0)], 1),
        (vec![fractional, sample(1, "candidate", 1.0)], 1),
        (
            vec![sample(1, "unrecognized", 1.0), sample(1, "candidate", 1.0)],
            1,
        ),
        (vec![], 0),
    ] {
        assert!(aggregate(&rows, pairs).is_err());
    }
}

#[test]
fn invalid_child_or_wrong_engine_is_rejected() {
    for field in ["valid", "forks", "target", "non_success_cells"] {
        let mut candidate = sample(1, "candidate", 1.0);
        match field {
            "valid" => candidate["summary"]["valid"] = json!(false),
            "forks" => {
                let f = candidate["summary"]["forks"][0].clone();
                candidate["summary"]["forks"]
                    .as_array_mut()
                    .unwrap()
                    .push(f);
            }
            "target" => candidate["summary"]["forks"][0]["target"] = json!("jvm-planning"),
            _ => candidate["summary"]["forks"][0]["non_success_cells"] = json!([{"failures":1}]),
        }
        assert!(
            aggregate(&[sample(1, "baseline", 1.0), candidate], 1).is_err(),
            "{field}"
        );
    }
}

#[test]
fn settings_grids_skips_counts_and_windows_must_match() {
    for mutation in 0..10 {
        let mut candidate = sample(1, "candidate", 1.0);
        let f = &mut candidate["summary"]["forks"][0];
        match mutation {
            0 => f["settings"]["requests"] = json!(64),
            1 => f["groups"] = json!([]),
            2 => {
                let g = f["groups"][0].clone();
                f["groups"].as_array_mut().unwrap().push(g);
            }
            3 => f["groups"][0]["case"] = json!("different"),
            4 => f["skipped_cells"] = json!([{"case":"missing","reason":"budget"}]),
            5 => f["groups"][0]["requests"] = json!(128),
            6 => f["groups"][0]["latency_window"] = json!("different"),
            7 => f["groups"][0]["rounds"][1]["repetition"] = json!(2),
            8 => f["groups"][0]["rounds"][1]["requests"] = json!(64),
            _ => {
                f["groups"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("byte_accounting");
            }
        }
        assert!(
            aggregate(&[sample(1, "baseline", 1.0), candidate], 1).is_err(),
            "{mutation}"
        );
    }
}

#[test]
fn cell_counts_cannot_drift_together_in_a_later_pair() {
    let mut b = sample(2, "baseline", 1.0);
    let mut c = sample(2, "candidate", 1.0);
    b["summary"]["forks"][0]["groups"][0]["requests"] = json!(128);
    c["summary"]["forks"][0]["groups"][0]["requests"] = json!(128);
    assert!(aggregate(
        &[
            sample(1, "baseline", 1.0),
            sample(1, "candidate", 1.0),
            b,
            c
        ],
        2
    )
    .is_err());
}

#[test]
fn invalid_or_absent_metrics_are_rejected() {
    for (field, value) in [
        ("requests_per_s", json!(0)),
        ("p95_ms", json!("NaN")),
        ("median_ms", Value::Null),
        ("client_cpu_ns", json!(false)),
        ("completed_round_requests_per_s", Value::Null),
    ] {
        let mut c = sample(1, "candidate", 1.0);
        c["summary"]["forks"][0]["groups"][0][field] = value;
        assert!(
            aggregate(&[sample(1, "baseline", 1.0), c], 1).is_err(),
            "{field}"
        );
    }
}

#[test]
fn performance_candidate_requires_improvement_and_every_guard() {
    let cells = cells(1.25);
    assert_eq!(
        gate(&cells, Some(&policy()), false).unwrap()["status"],
        "performance_candidate"
    );
    assert_eq!(
        gate(&self::cells(1.0), Some(&policy()), false).unwrap()["status"],
        "rejected"
    );
    for (metric, value) in [
        ("median_latency", 1.3),
        ("throughput", 0.7),
        ("p95_latency", 1.6),
        ("client_cpu_per_request", 1.3),
        ("completed_round_throughput", 0.7),
    ] {
        let mut bad = cells.clone();
        bad[0]["ratios"][metric]["geometric_mean"] = json!(value);
        assert_eq!(
            gate(&bad, Some(&policy()), false).unwrap()["passed"],
            false,
            "{metric}"
        );
    }
}

#[test]
fn noise_control_skips_improvement_but_checks_all_guards() {
    let mut cells = cells(1.0);
    assert_eq!(
        gate(&cells, Some(&policy()), true).unwrap()["status"],
        "noise_control_pass"
    );
    cells[0]["ratios"]["completed_round_throughput"]["geometric_mean"] = json!(0.7);
    assert_eq!(
        gate(&cells, Some(&policy()), true).unwrap()["passed"],
        false
    );
}

#[test]
fn requiring_improvement_in_every_pair_is_optional() {
    let cells = aggregate(
        &[
            sample(1, "baseline", 1.0),
            sample(1, "candidate", 2.0),
            sample(2, "baseline", 1.0),
            sample(2, "candidate", 0.9),
        ],
        2,
    )
    .unwrap();
    assert_eq!(
        gate(&cells, Some(&policy()), false).unwrap()["passed"],
        true
    );
    let mut strict = policy();
    strict["require_all_pairs_target_improve"] = json!(true);
    assert_eq!(gate(&cells, Some(&strict), false).unwrap()["passed"], false);
}

#[test]
fn sharing_topologies_do_not_inflate_distinct_target_count() {
    let mut cells = cells(1.25);
    let mut duplicate = cells[0].clone();
    duplicate["session_model"] = json!("isolated");
    cells.push(duplicate);
    let mut p = policy();
    p["min_improved_targets"] = json!(2);
    let decision = gate(&cells, Some(&p), false).unwrap();
    assert_eq!(decision["passed"], false);
    assert_eq!(decision["improved_targets"].as_array().unwrap().len(), 1);
    cells[1]["ratios"]["throughput"]["geometric_mean"] = json!(1.0);
    assert_eq!(
        gate(&cells, Some(&policy()), false).unwrap()["improved_targets"],
        json!([])
    );
}

#[test]
fn invalid_thresholds_missing_targets_and_empty_cells_are_rejected() {
    let cells = cells(1.25);
    for (key, value) in [
        ("targets", json!([])),
        ("targets", json!([{"case":"absent","concurrency":[8]}])),
        ("min_improved_targets", json!(1.5)),
        ("min_target_throughput_ratio", json!(1)),
        ("max_p95_latency_ratio", Value::Null),
        ("require_all_pairs_target_improve", json!("false")),
    ] {
        let mut p = policy();
        p[key] = value;
        assert!(gate(&cells, Some(&p), false).is_err(), "{key}");
    }
    assert_eq!(gate(&cells, None, false).unwrap()["status"], "descriptive");
    assert!(gate(&[], None, false).is_err());
}

#[test]
fn policy_matrix_rejects_narrowed_settings_and_hidden_filter() {
    let mut matrix = json!({"profile":"smoke","concurrency":[1,8,32],"case_filter":null});
    validate_matrix(
        Some(&json!({"matrix":{"profile":"smoke","concurrency":[1,8,32]}})),
        &matrix,
    )
    .unwrap();
    for expected in [
        json!({"profile":"standard"}),
        json!({"concurrency":[1]}),
        json!({"unknown":1}),
        json!({}),
    ] {
        assert!(validate_matrix(Some(&json!({"matrix":expected})), &matrix).is_err());
    }
    matrix["case_filter"] = json!("range");
    assert!(validate_matrix(Some(&json!({"matrix":{"profile":"smoke"}})), &matrix).is_err());
}

#[test]
fn frozen_hashes_detect_source_and_snapshot_mutation() {
    let root = Temp::new();
    let input = root.0.join("input");
    fs::write(&input, "original").unwrap();
    let mut inputs = Vec::new();
    let snapshot = freeze(&input, &root.0.join("frozen/item"), &mut inputs).unwrap();
    verify(&inputs).unwrap();
    fs::write(&input, "changed").unwrap();
    assert!(verify(&inputs).is_err());
    fs::write(&input, "original").unwrap();
    fs::write(snapshot, "changed").unwrap();
    assert!(verify(&inputs).is_err());
}

#[test]
fn nonfinite_json_policy_is_rejected() {
    let root = Temp::new();
    let path = root.0.join("policy.json");
    for text in [
        r#"{"invalid":NaN}"#,
        r#"{"invalid":Infinity}"#,
        r#"{"invalid":1e999}"#,
    ] {
        fs::write(&path, text).unwrap();
        assert!(read_json(&path).is_err());
    }
}

#[test]
fn failed_child_preserves_evidence_and_stops_later_pairs() {
    let root = Temp::new();
    let options = options(&root.0, false);
    let output = options.output.clone();
    let mut calls = 0;
    assert!(!run_with(options, |_, _, _| {
        calls += 1;
        Ok(1)
    })
    .unwrap());
    let result = read_json(&output.join("compare.json")).unwrap();
    assert_eq!(result["valid"], false);
    assert_eq!(result["runs"].as_array().unwrap().len(), 1);
    assert_eq!(calls, 2);
    assert_eq!(result["runs"][0]["report_exit_code"], 1);
    assert!(output.join("COMPARE.md").is_file());
}

#[test]
fn matrix_failure_is_saved_before_any_child_starts() {
    let root = Temp::new();
    let options = options(&root.0, false);
    let output = options.output.clone();
    let mut p = policy();
    p["matrix"] = json!({"requests":256});
    save(options.policy.as_ref().unwrap(), &p).unwrap();
    let mut calls = 0;
    assert!(!run_with(options, |_, _, _| {
        calls += 1;
        Ok(0)
    })
    .unwrap());
    assert_eq!(calls, 0);
    assert!(output.join("COMPARE.md").is_file());
}

#[test]
fn noise_control_rejects_different_bytes_without_child_launch() {
    let root = Temp::new();
    let mut options = options(&root.0, true);
    let output = options.output.clone();
    options.candidate_library = root.0.join("different");
    fs::write(&options.candidate_library, "different").unwrap();
    let mut calls = 0;
    assert!(!run_with(options, |_, _, _| {
        calls += 1;
        Ok(0)
    })
    .unwrap());
    assert_eq!(calls, 0);
    assert!(fs::read_to_string(output.join("compare.json"))
        .unwrap()
        .contains("identical"));
}

#[test]
fn full_mocked_control_uses_frozen_rust_harness_and_alternating_children() {
    let root = Temp::new();
    let options = options(&root.0, true);
    let output = options.output.clone();
    let mut labels = Vec::new();
    assert!(run_with(options, |command, _, _| {
        assert!(Path::new(&command[0])
            .components()
            .any(|p| p.as_os_str() == "harness"));
        assert_ne!(command[0], "python3");
        if command[1] == "suite" {
            let child =
                PathBuf::from(&command[command.iter().position(|s| s == "--output").unwrap() + 1]);
            fs::create_dir(&child).unwrap();
            labels.push(child.file_name().unwrap().to_str().unwrap().to_owned());
            assert!(command.iter().any(|s| s == "rust-jvm-planning"));
        } else {
            assert_eq!(command[1], "report");
            assert_eq!(command[2], "--input");
            let child = PathBuf::from(&command[3]);
            let name = child.file_name().unwrap().to_str().unwrap();
            let variant = name.split_once('-').unwrap().1;
            let mut summary = sample(1, variant, 1.0)["summary"].clone();
            summary["analysis_reporter_sha256"] = json!(sha_file(Path::new(&command[0])).unwrap());
            save(&child.join("summary.json"), &summary).unwrap();
        }
        Ok(0)
    })
    .unwrap());
    assert_eq!(
        labels,
        vec![
            "pair1-baseline",
            "pair1-candidate",
            "pair2-candidate",
            "pair2-baseline"
        ]
    );
    let result = read_json(&output.join("compare.json")).unwrap();
    assert_eq!(result["valid"], true);
    assert_eq!(result["status"], "noise_control_pass");
    assert_eq!(result["cells"][0]["pairs"].as_array().unwrap().len(), 2);
    verify(&serde_json::from_value::<Vec<FrozenInput>>(result["inputs"].clone()).unwrap()).unwrap();
}

#[test]
fn summary_from_another_reporter_is_rejected() {
    let root = Temp::new();
    let options = options(&root.0, true);
    let output = options.output.clone();
    let mut calls = 0;
    assert!(!run_with(options, |command, _, _| {
        calls += 1;
        if command[1] == "suite" {
            fs::create_dir(Path::new(
                &command[command.iter().position(|s| s == "--output").unwrap() + 1],
            ))
            .unwrap();
        } else {
            let mut summary = sample(1, "baseline", 1.0)["summary"].clone();
            summary["analysis_reporter_sha256"] = json!("wrong");
            save(&Path::new(&command[3]).join("summary.json"), &summary).unwrap();
        }
        Ok(0)
    })
    .unwrap());
    assert_eq!(calls, 2);
    assert!(fs::read_to_string(output.join("compare.json"))
        .unwrap()
        .contains("frozen Rust reporter"));
}

#[test]
fn payload_manifest_path_traversal_and_symlink_escape_are_rejected() {
    for name in ["../outside", "/absolute", ""] {
        let root = Temp::new();
        let source = root.0.join("source");
        fs::create_dir(&source).unwrap();
        save(
            &source.join("manifest.json"),
            &json!({"payloads":[{"arrow_file":name}],"cases":[]}),
        )
        .unwrap();
        assert!(copy_payloads(&source, &root.0.join("copy")).is_err());
    }
    #[cfg(unix)]
    {
        let root = Temp::new();
        let source = root.0.join("source");
        fs::create_dir(&source).unwrap();
        fs::write(root.0.join("outside"), "private").unwrap();
        std::os::unix::fs::symlink(root.0.join("outside"), source.join("inside")).unwrap();
        save(
            &source.join("manifest.json"),
            &json!({"payloads":[{"arrow_file":"inside"}],"cases":[]}),
        )
        .unwrap();
        assert!(copy_payloads(&source, &root.0.join("copy"))
            .unwrap_err()
            .contains("symlink"));
    }
}

#[test]
fn options_preserve_matrix_and_reject_unknown_duplicate_or_out_of_range_values() {
    let mut map = BTreeMap::from([
        ("--output".into(), "output".into()),
        ("--baseline-jar".into(), "base.jar".into()),
        ("--baseline-library".into(), "base.so".into()),
        ("--candidate-jar".into(), "candidate.jar".into()),
        ("--candidate-library".into(), "candidate.so".into()),
        ("--spark-home".into(), "spark".into()),
        ("--java-home".into(), "java".into()),
    ]);
    let options = CompareOptions::from_options(&map).unwrap();
    assert_eq!(options.effective_matrix()["case_filter"], Value::Null);
    assert_eq!(options.effective_matrix()["concurrency"], json!([1, 8, 32]));
    assert!(options.output.is_absolute());
    for (key, value) in [
        ("--unknown", "x"),
        ("--concurrency", "1,01"),
        ("--session-models", "shared,shared"),
        ("--concurrency", "513"),
        ("--pairs", "0"),
        ("--requests", "1"),
        ("--control", "maybe"),
        ("--warmups", "-1"),
    ] {
        let mut bad = map.clone();
        bad.insert(key.into(), value.into());
        assert!(CompareOptions::from_options(&bad).is_err(), "{key}={value}");
    }
    map.insert("--control".into(), "true".into());
    assert!(CompareOptions::from_options(&map).unwrap().control);
}

#[test]
fn invalid_programmatic_options_do_not_create_output_or_start_children() {
    for mutation in 0..6 {
        let root = Temp::new();
        let mut options = options(&root.0, false);
        let output = options.output.clone();
        match mutation {
            0 => options.pairs = 0,
            1 => options.concurrency.clear(),
            2 => options.requests = 0,
            3 => options.session_models = vec!["shared".into(), "shared".into()],
            4 => options.max_prepared_mib = usize::MAX,
            _ => options.output = PathBuf::from("relative-output"),
        }
        assert!(run_with(options, |_, _, _| panic!("invalid options started a child")).is_err());
        assert!(!output.exists());
    }
}

use super::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

fn cell(case: &str, latency: u64, elapsed: u64, unsupported: u64) -> Value {
    json!({"case":case,"workload":"AnalyzePlan","session_model":"shared","channel_model":"shared","concurrency":2,"repetition":0,"requests":4,"successes":4-unsupported,"failures":0,"unsupported":unsupported,"elapsed_ns":elapsed,"latencies_ns":[latency,latency,latency,latency],"first_message_latencies_ns":[],"request_bytes":32,"response_bytes":64,"status_counts":if unsupported==0 {json!({"Ok":4})} else {json!({"Unimplemented":4})},"validation_errors":[],"cleanup_errors":[],"client_cpu_ns":200,"warmup_validation_errors":[],"warmup_cleanup_errors":[],"latency_window":"primary_rpc_drain_including_client_encode","throughput_window":"primary_rpc_round_excluding_preparation_validation_cleanup"})
}

fn skip(case: &str, repetition: u64) -> Value {
    json!({"case":case,"session_model":"shared","channel_model":"shared","concurrency":2,"repetition":repetition,"reason":"prepared request/response memory budget"})
}

struct Fixture {
    directory: PathBuf,
    clients: BTreeMap<String, Value>,
    manifest: Value,
}

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "protocol-rust-report-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        fs::create_dir(directory.join("payloads")).unwrap();
        fs::write(
            directory.join("payloads/manifest.json"),
            b"{\"version\":1,\"cases\":[],\"payloads\":[]}",
        )
        .unwrap();
        fs::create_dir(directory.join("artifacts")).unwrap();
        fs::write(directory.join("artifacts/binary"), b"frozen binary").unwrap();
        let mut clients = BTreeMap::new();
        let mut runs = Vec::new();
        for (target, latency, elapsed) in [("rust-fixture", 100, 1000), ("jvm-fixture", 200, 2000)]
        {
            let name = format!("fork1-{target}");
            fs::create_dir(directory.join(&name)).unwrap();
            clients.insert(name.clone(),json!({"schema_version":1,"passed":true,"mode":"fixture","settings":{"no_execution":true,"retries":0,"requests":4,"coverage_only":false,"repetitions":1,"concurrency":[2],"session_models":["shared"],"channel_models":["shared"],"endpoint":format!("http://127.0.0.1:{}",1234+latency)},"coverage":{"passed":true,"records":ROUTES.iter().map(|route|json!({"route":route,"status":"OK","passed":true,"supported":true,"tested":true})).collect::<Vec<_>>()},"capabilities":[{"case":"range","rpc":"AnalyzePlan","supported":true}],"cells":[cell("range",latency,elapsed,0)],"skipped_cells":[]}));
            runs.push(json!({"fork_id":name,"fork":1,"target":target,"profile_kind":"fixture","client_json":format!("{name}/client.json"),"clean_shutdown":true,"client_exit_code":0,"shutdown":{"listener_closed":true,"fixture_stats":{"query_executions":0}}}));
        }
        let manifest = json!({"schema_version":1,"complete":true,"runs":runs,"metadata":{"forks_per_target":1,"targets":["rust-fixture","jvm-fixture"],"artifacts":{"binary":{"snapshot":"/old/run/artifacts/binary","sha256":sha_file(&directory.join("artifacts/binary")).unwrap()}},"payload_sha256":{"manifest.json":sha_file(&directory.join("payloads/manifest.json")).unwrap()}}});
        Self {
            directory,
            clients,
            manifest,
        }
    }
    fn rust(&mut self) -> &mut Value {
        self.clients.get_mut("fork1-rust-fixture").unwrap()
    }
    fn jvm(&mut self) -> &mut Value {
        self.clients.get_mut("fork1-jvm-fixture").unwrap()
    }
    fn flush(&self) {
        save(&self.directory.join("manifest.json"), &self.manifest).unwrap();
        for (name, client) in &self.clients {
            save(&self.directory.join(name).join("client.json"), client).unwrap();
        }
    }
    fn summary(&self) -> Value {
        self.flush();
        summarize(&self.directory).unwrap()
    }
    fn invalid(&self) -> Value {
        let value = self.summary();
        assert_eq!(value["valid"], false, "{value}");
        value
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
}

fn close(actual: &Value, expected: f64) {
    assert!(
        (actual.as_f64().unwrap() - expected).abs() < 1e-9,
        "{actual} != {expected}"
    );
}

#[test]
fn matched_ratios_and_csv() {
    let f = Fixture::new();
    let result = f.summary();
    assert_eq!(result["valid"], true, "{}", result["errors"]);
    assert_eq!(result["comparisons"].as_array().unwrap().len(), 1);
    close(
        &result["comparisons"][0]["latency_ratio"]["geometric_mean"],
        0.5,
    );
    close(
        &result["comparisons"][0]["throughput_ratio"]["geometric_mean"],
        2.0,
    );
    assert_eq!(
        result["analysis_reporter_sha256"],
        sha_file(&std::env::current_exe().unwrap()).unwrap()
    );
    write_csvs(&f.directory, &result).unwrap();
    assert_eq!(
        fs::read_to_string(f.directory.join("cells.csv"))
            .unwrap()
            .lines()
            .count(),
        3
    );
    let csv = fs::read_to_string(f.directory.join("comparisons.csv")).unwrap();
    assert_eq!(csv.lines().nth(1).unwrap().split(',').nth(8), Some("0.5"));
    assert!(markdown(&result).contains("valid"));
}

#[test]
fn unsupported_excluded_and_preserved() {
    let mut f = Fixture::new();
    for client in f.clients.values_mut() {
        client["capabilities"]
            .as_array_mut()
            .unwrap()
            .push(json!({"case":"get_status","rpc":"AnalyzePlan","supported":true}));
    }
    f.rust()["cells"]
        .as_array_mut()
        .unwrap()
        .push(cell("get_status", 100, 1000, 0));
    f.jvm()["cells"]
        .as_array_mut()
        .unwrap()
        .push(cell("get_status", 100, 1000, 4));
    let result = f.summary();
    assert_eq!(result["valid"], true, "{}", result["errors"]);
    assert_eq!(result["comparisons"].as_array().unwrap().len(), 1);
    assert_eq!(result["forks"][0]["groups"].as_array().unwrap().len(), 2);
    assert_eq!(result["forks"][1]["groups"].as_array().unwrap().len(), 1);
    assert_eq!(result["forks"][1]["non_success_cells"][0]["unsupported"], 4);
}

#[test]
fn completed_round_rate_is_separate() {
    let mut f = Fixture::new();
    for client in f.clients.values_mut() {
        let c = &mut client["cells"][0];
        c["preparation_ns"] = json!(1000);
        c["validation_cleanup_ns"] = json!(2000);
        c["full_round_elapsed_ns"] = json!(c["elapsed_ns"].as_u64().unwrap() + 3000);
    }
    let result = f.summary();
    assert_eq!(result["valid"], true, "{}", result["errors"]);
    let group = &result["forks"][0]["groups"][0];
    close(&group["requests_per_s"], 4_000_000.0);
    close(&group["completed_round_requests_per_s"], 1_000_000.0);
    close(
        &result["comparisons"][0]["throughput_ratio"]["geometric_mean"],
        2.0,
    );
    close(
        &result["comparisons"][0]["completed_round_throughput_ratio"]["geometric_mean"],
        1.25,
    );
    write_csvs(&f.directory, &result).unwrap();
    assert!(fs::read_to_string(f.directory.join("cells.csv"))
        .unwrap()
        .contains("1000000.0"));
    assert!(markdown(&result).contains("Completed-round"));
}

#[test]
fn bad_completed_round_components() {
    let mut f = Fixture::new();
    let c = &mut f.rust()["cells"][0];
    c["preparation_ns"] = json!(100);
    c["validation_cleanup_ns"] = json!(200);
    c["full_round_elapsed_ns"] = json!(1001);
    f.invalid();
}
#[test]
fn first_arrow_latency_is_distinct() {
    let mut f = Fixture::new();
    f.rust()["cells"][0]["first_message_latencies_ns"] = json!([10, 10, 10, 10]);
    f.rust()["cells"][0]["first_arrow_latencies_ns"] = json!([20, 20, 20, 20]);
    let result = f.summary();
    assert_eq!(result["valid"], true);
    close(
        &result["forks"][0]["groups"][0]["first_message_median_ms"],
        0.00001,
    );
    close(
        &result["forks"][0]["groups"][0]["first_arrow_median_ms"],
        0.00002,
    );
}
#[test]
fn excess_first_arrow_samples_rejected() {
    let mut f = Fixture::new();
    f.rust()["cells"][0]["first_arrow_latencies_ns"] = json!([20, 20, 20, 20, 20]);
    f.invalid();
}
#[test]
fn mixed_completed_round_counters() {
    let mut f = Fixture::new();
    let mut c = f.rust()["cells"][0].clone();
    c["repetition"] = json!(1);
    c["full_round_elapsed_ns"] = json!(5000);
    f.rust()["cells"].as_array_mut().unwrap().push(c);
    f.invalid();
}
#[test]
fn bad_counts() {
    let mut f = Fixture::new();
    f.rust()["cells"][0]["successes"] = json!(3);
    f.invalid();
}
#[test]
fn bad_status_counts() {
    let mut f = Fixture::new();
    f.rust()["cells"][0]["status_counts"] = json!({"Ok":3});
    f.invalid();
}
#[test]
fn bad_latency_count() {
    let mut f = Fixture::new();
    f.rust()["cells"][0]["latencies_ns"] = json!([100]);
    f.invalid();
}
#[test]
fn duplicate_cell() {
    let mut f = Fixture::new();
    let c = f.rust()["cells"][0].clone();
    f.rust()["cells"].as_array_mut().unwrap().push(c);
    f.invalid();
}
#[test]
fn settings_mismatch() {
    let mut f = Fixture::new();
    f.rust()["settings"]["requests"] = json!(8);
    assert_eq!(f.invalid()["comparisons"], json!([]));
}
#[test]
fn repetition_identity_mismatch() {
    let mut f = Fixture::new();
    f.rust()["cells"][0]["repetition"] = json!(1);
    assert_eq!(f.invalid()["comparisons"], json!([]));
}
#[test]
fn missing_fork() {
    let mut f = Fixture::new();
    f.manifest["metadata"]["forks_per_target"] = json!(2);
    f.invalid();
}
#[test]
fn corrupt_artifact_hash() {
    let f = Fixture::new();
    fs::write(f.directory.join("artifacts/binary"), b"different").unwrap();
    f.invalid();
}
#[test]
fn corrupt_corpus_hash() {
    let f = Fixture::new();
    fs::write(f.directory.join("payloads/manifest.json"), b"{}").unwrap();
    f.invalid();
}
#[test]
fn incomplete_run() {
    let mut f = Fixture::new();
    f.manifest["complete"] = json!(false);
    f.invalid();
}
#[test]
fn failed_client() {
    let mut f = Fixture::new();
    f.rust()["passed"] = json!(false);
    let result = f.invalid();
    assert!(result["errors"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| text(e).contains("fork1-rust-fixture/client.json")));
    assert!(markdown(&result).contains("raw `client.json`"));
}
#[test]
fn missing_route() {
    let mut f = Fixture::new();
    f.rust()["coverage"]["records"]
        .as_array_mut()
        .unwrap()
        .pop();
    f.invalid();
}
#[test]
fn nonzero_query_audit() {
    let mut f = Fixture::new();
    f.manifest["runs"][0]["shutdown"]["fixture_stats"]["query_executions"] = json!(1);
    f.invalid();
}
#[test]
fn missing_shutdown_audit() {
    let mut f = Fixture::new();
    f.manifest["runs"][0]["shutdown"]["listener_closed"] = json!(false);
    f.invalid();
}
#[test]
fn nonempty_final_cleanup_errors() {
    let mut f = Fixture::new();
    f.rust()["final_cleanup_errors"] = json!(["release failed"]);
    f.invalid();
}
#[test]
fn incomplete_client() {
    let mut f = Fixture::new();
    f.rust()["incomplete"] = json!(true);
    f.invalid();
}
#[test]
fn in_progress_client() {
    let mut f = Fixture::new();
    f.rust()["in_progress"] = json!({"case":"range","phase":"measuring"});
    f.invalid();
}
#[test]
fn skipped_budget_cells_preserved() {
    let mut f = Fixture::new();
    f.rust()["capabilities"]
        .as_array_mut()
        .unwrap()
        .push(json!({"case":"huge","rpc":"AnalyzePlan","supported":true}));
    let skipped = skip("huge", 0);
    f.rust()["skipped_cells"]
        .as_array_mut()
        .unwrap()
        .push(skipped.clone());
    let result = f.summary();
    assert_eq!(result["valid"], true, "{}", result["errors"]);
    assert_eq!(result["forks"][0]["skipped_cells"], json!([skipped]));
}
#[test]
fn symmetric_missing_repetitions_rejected() {
    let mut f = Fixture::new();
    for c in f.clients.values_mut() {
        c["settings"]["repetitions"] = json!(2);
    }
    let result = f.invalid();
    assert_eq!(result["comparisons"], json!([]));
    assert!(result["errors"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| text(e).contains("1 missing")));
    assert!(result["errors"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| text(e).contains("fork1-rust-fixture/client.json")));
}
#[test]
fn symmetric_settings_narrower_than_manifest_rejected() {
    let mut f = Fixture::new();
    f.manifest["metadata"]["repetitions"] = json!(2);
    f.invalid();
}
#[test]
fn manifest_csv_grid_settings_match_client_arrays() {
    let mut f = Fixture::new();
    f.manifest["metadata"]["concurrency"] = json!("2");
    f.manifest["metadata"]["session_models"] = json!("shared");
    f.manifest["metadata"]["channel_models"] = json!("shared");
    assert_eq!(f.summary()["valid"], true);
    f.manifest["metadata"]["concurrency"] = json!("1,2");
    f.invalid();
}
#[test]
fn symmetric_missing_topologies_rejected() {
    let mut f = Fixture::new();
    for c in f.clients.values_mut() {
        c["settings"]["session_models"] = json!(["shared", "isolated"]);
    }
    f.invalid();
}
#[test]
fn symmetric_missing_concurrency_rejected() {
    let mut f = Fixture::new();
    for c in f.clients.values_mut() {
        c["settings"]["concurrency"] = json!([1, 2]);
    }
    f.invalid();
}
#[test]
fn symmetric_out_of_range_repetitions_rejected() {
    let mut f = Fixture::new();
    for c in f.clients.values_mut() {
        c["cells"][0]["repetition"] = json!(1);
    }
    f.invalid();
}
#[test]
fn symmetric_wrong_request_count_rejected() {
    let mut f = Fixture::new();
    for c in f.clients.values_mut() {
        c["settings"]["requests"] = json!(8);
    }
    f.invalid();
}
#[test]
fn empty_timed_grid_rejected() {
    let mut f = Fixture::new();
    for c in f.clients.values_mut() {
        c["cells"] = json!([]);
    }
    f.invalid();
}
#[test]
fn missing_capabilities_rejected() {
    let mut f = Fixture::new();
    f.rust()["capabilities"] = json!([]);
    f.invalid();
}
#[test]
fn duplicate_capabilities_rejected() {
    let mut f = Fixture::new();
    let c = f.rust()["capabilities"][0].clone();
    f.rust()["capabilities"].as_array_mut().unwrap().push(c);
    f.invalid();
}
#[test]
fn unsupported_capability_requires_no_timed_cells() {
    let mut f = Fixture::new();
    for c in f.clients.values_mut() {
        c["capabilities"]
            .as_array_mut()
            .unwrap()
            .push(json!({"case":"unavailable","rpc":"GetStatus","supported":false}));
    }
    assert_eq!(f.summary()["valid"], true);
    f.rust()["cells"]
        .as_array_mut()
        .unwrap()
        .push(cell("unavailable", 100, 1000, 0));
    f.invalid();
}
#[test]
fn explicit_skip_completes_requested_grid() {
    let mut f = Fixture::new();
    for c in f.clients.values_mut() {
        c["settings"]["repetitions"] = json!(2);
        c["skipped_cells"]
            .as_array_mut()
            .unwrap()
            .push(skip("range", 1));
    }
    assert_eq!(f.summary()["valid"], true);
}
#[test]
fn all_cells_explicitly_skipped_is_valid() {
    let mut f = Fixture::new();
    for c in f.clients.values_mut() {
        c["cells"] = json!([]);
        c["skipped_cells"]
            .as_array_mut()
            .unwrap()
            .push(skip("range", 0));
    }
    assert_eq!(f.summary()["valid"], true);
}
#[test]
fn duplicate_skips_rejected() {
    let mut f = Fixture::new();
    f.rust()["cells"] = json!([]);
    f.rust()["skipped_cells"] = json!([skip("range", 0), skip("range", 0)]);
    f.invalid();
}
#[test]
fn measured_and_skipped_cell_rejected() {
    let mut f = Fixture::new();
    f.rust()["skipped_cells"]
        .as_array_mut()
        .unwrap()
        .push(skip("range", 0));
    f.invalid();
}
#[test]
fn skip_without_reason_rejected() {
    let mut f = Fixture::new();
    f.rust()["cells"] = json!([]);
    f.rust()["skipped_cells"] = json!([skip("range", 0)]);
    f.rust()["skipped_cells"][0]["reason"] = json!("");
    f.invalid();
}
#[test]
fn explicit_coverage_only_is_valid() {
    let mut f = Fixture::new();
    for c in f.clients.values_mut() {
        c["settings"]["coverage_only"] = json!(true);
        c["cells"] = json!([]);
        c["capabilities"] = json!([]);
    }
    let result = f.summary();
    assert_eq!(result["valid"], true);
    assert_eq!(result["comparisons"], json!([]));
}
#[test]
fn coverage_only_with_timed_work_rejected() {
    let mut f = Fixture::new();
    for c in f.clients.values_mut() {
        c["settings"]["coverage_only"] = json!(true);
    }
    f.invalid();
}
#[test]
fn coverage_only_with_skipped_work_rejected() {
    let mut f = Fixture::new();
    f.rust()["settings"]["coverage_only"] = json!(true);
    f.rust()["cells"] = json!([]);
    f.rust()["capabilities"] = json!([]);
    f.rust()["skipped_cells"] = json!([skip("range", 0)]);
    f.invalid();
}
#[test]
fn missing_coverage_only_setting_rejected() {
    let mut f = Fixture::new();
    f.rust()["settings"]
        .as_object_mut()
        .unwrap()
        .remove("coverage_only");
    f.invalid();
}

// Rust-specific regression checks supplement the 45 original reporter cases.
#[test]
fn rust_orchestration_hashes_are_checked_without_python_snapshots() {
    let mut f = Fixture::new();
    fs::write(
        f.directory.join("artifacts/harness"),
        b"compiled rust harness",
    )
    .unwrap();
    let hash = sha_file(&f.directory.join("artifacts/harness")).unwrap();
    f.manifest["metadata"]["orchestration"] = json!("rust");
    f.manifest["metadata"]["runner_sha256"] = json!(hash);
    f.manifest["metadata"]["reporter_sha256"] = json!(hash);
    assert_eq!(f.summary()["valid"], true);
    fs::write(f.directory.join("artifacts/harness"), b"changed").unwrap();
    f.invalid();
}
#[test]
fn legacy_orchestration_hashes_are_still_checked() {
    let mut f = Fixture::new();
    for (field, name) in [
        ("runner_sha256", "runner.py"),
        ("reporter_sha256", "reporter.py"),
    ] {
        fs::write(f.directory.join("artifacts").join(name), b"frozen source").unwrap();
        f.manifest["metadata"][field] =
            json!(sha_file(&f.directory.join("artifacts").join(name)).unwrap());
    }
    assert_eq!(f.summary()["valid"], true);
    fs::write(f.directory.join("artifacts/reporter.py"), b"changed").unwrap();
    f.invalid();
}
#[test]
fn invalid_and_malformed_runs_still_write_all_four_outputs() {
    let mut f = Fixture::new();
    f.rust()["passed"] = json!(false);
    f.flush();
    assert!(!report(&f.directory).unwrap());
    for name in ["summary.json", "REPORT.md", "cells.csv", "comparisons.csv"] {
        assert!(f.directory.join(name).is_file());
    }
    fs::write(f.directory.join("manifest.json"), b"{broken").unwrap();
    assert!(!report(&f.directory).unwrap());
    assert_eq!(
        read_json(&f.directory.join("summary.json")).unwrap()["valid"],
        false
    );
}
#[test]
fn escaping_paths_and_invalid_numeric_values_are_rejected() {
    for bad in [json!(true), json!(-1), json!(1.5), json!("4")] {
        let mut f = Fixture::new();
        f.rust()["cells"][0]["requests"] = bad;
        f.invalid();
    }
    let mut f = Fixture::new();
    f.manifest["runs"][0]["client_json"] = json!("../outside.json");
    f.invalid();
    let mut f = Fixture::new();
    f.manifest["metadata"]["payload_sha256"]["../outside"] = json!("hash");
    f.invalid();
}
#[test]
fn planning_route_and_job_audits_are_enforced() {
    let mut f = Fixture::new();
    for c in f.clients.values_mut() {
        c["mode"] = json!("planning");
        c["coverage"]["records"] = json!([{"route":"AnalyzePlan","passed":true},{"route":"Config","passed":true},{"route":"ReleaseSession","passed":true}]);
    }
    for record in f.manifest["runs"].as_array_mut().unwrap() {
        record["profile_kind"] = json!("planning");
        record["shutdown"]["spark_job_audit"] = json!({"jobs_started":0});
    }
    assert_eq!(f.summary()["valid"], true);
    f.manifest["runs"][0]["shutdown"]["spark_job_audit"]["jobs_started"] = json!(1);
    f.invalid();
    f.manifest["runs"][0]["shutdown"]["spark_job_audit"]["jobs_started"] = json!(0);
    f.rust()["coverage"]["records"]
        .as_array_mut()
        .unwrap()
        .push(json!({"route":"ExecutePlan","passed":true}));
    f.invalid();
}
#[test]
fn csv_escapes_quotes_commas_and_newlines() {
    assert_eq!(
        csv_field(&json!("a,\"quoted\"\ncase")),
        "\"a,\"\"quoted\"\"\ncase\""
    );
    assert_eq!(csv_field(&Value::Null), "");
}
#[test]
fn quantiles_and_geometric_means_match_descriptive_contract() {
    let mut values = vec![1.0, 4.0, 2.0, 3.0];
    assert_eq!(median(&mut values), Some(2.5));
    let mut f = Fixture::new();
    f.rust()["cells"][0]["latencies_ns"] = json!([100, 200, 300, 400]);
    let result = f.summary();
    let group = &result["forks"][0]["groups"][0];
    close(&group["median_ms"], 0.00025);
    close(&group["p95_ms"], 0.0004);
    close(&group["p99_ms"], 0.0004);
}

#[test]
fn integral_float_repetitions_sort_numerically() {
    let mut f = Fixture::new();
    for client in f.clients.values_mut() {
        client["settings"]["repetitions"] = json!(2);
        client["cells"][0]["repetition"] = json!(1.0);
        let mut earlier = client["cells"][0].clone();
        earlier["repetition"] = json!(0.0);
        client["cells"].as_array_mut().unwrap().push(earlier);
    }
    let result = f.summary();
    assert_eq!(result["valid"], true, "{}", result["errors"]);
    let rounds = &result["forks"][0]["groups"][0]["rounds"];
    assert_eq!(rounds[0]["repetition"].as_f64(), Some(0.0));
    assert_eq!(rounds[1]["repetition"].as_f64(), Some(1.0));
}

#[test]
fn rejected_fork_does_not_reserve_an_unused_fork_id() {
    let mut f = Fixture::new();
    let mut duplicate = f.manifest["runs"][0].clone();
    duplicate["fork_id"] = json!("later-unique-id");
    f.manifest["runs"]
        .as_array_mut()
        .unwrap()
        .push(duplicate.clone());
    duplicate["fork"] = json!(2);
    f.manifest["runs"].as_array_mut().unwrap().push(duplicate);
    let result = f.invalid();
    assert!(result["forks"]
        .as_array()
        .unwrap()
        .iter()
        .any(|fork| fork["fork_id"] == "later-unique-id" && fork["fork"] == 2));
}

#[test]
fn huge_incomplete_grid_is_rejected_without_materializing_requested_repetitions() {
    let mut f = Fixture::new();
    f.rust()["settings"]["repetitions"] = json!(1_000_000_000);
    let result = f.invalid();
    assert!(result["errors"]
        .as_array()
        .unwrap()
        .iter()
        .any(|error| text(error).contains("999999999 missing")));
}

#[test]
fn missing_artifact_digest_is_not_accepted() {
    let mut f = Fixture::new();
    f.manifest["metadata"]["artifacts"]["binary"]
        .as_object_mut()
        .unwrap()
        .remove("sha256");
    f.invalid();
}

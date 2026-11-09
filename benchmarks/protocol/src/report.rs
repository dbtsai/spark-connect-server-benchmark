//! Validate frozen benchmark runs and summarize matched fresh-server forks.
//!
//! The JSON contract also accepts reports produced by the original Python runner.

use crate::harness_util::{now_utc, save, sha_file};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path};
use std::sync::OnceLock;

const KEY_FIELDS: [&str; 5] = [
    "case",
    "workload",
    "session_model",
    "channel_model",
    "concurrency",
];
const ROUTES: [&str; 12] = [
    "ExecutePlan",
    "AnalyzePlan",
    "Config",
    "AddArtifacts",
    "ArtifactStatus",
    "Interrupt",
    "ReattachExecute",
    "ReleaseExecute",
    "ReleaseSession",
    "FetchErrorDetails",
    "CloneSession",
    "GetStatus",
];
const PAIR_TARGETS: [(&str, &str); 2] = [
    ("rust-fixture", "jvm-fixture"),
    ("rust-jvm-planning", "jvm-planning"),
];
type Key = (String, String, String, String, u64);
type CellIdentity = (Key, u64);

fn read_json(path: &Path) -> Result<Value, String> {
    let bytes = fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    serde_json::from_slice(&bytes).map_err(|e| format!("{}: {e}", path.display()))
}

fn object<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>, String> {
    value
        .as_object()
        .ok_or_else(|| format!("{label} is not an object"))
}

fn array<'a>(value: &'a Value, label: &str) -> Result<&'a [Value], String> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| format!("{label} is not an array"))
}

fn optional_array<'a>(value: &'a Value, field: &str, label: &str) -> Result<&'a [Value], String> {
    match value.get(field) {
        Some(values) => array(values, &format!("{label}.{field}")),
        None => Ok(&[]),
    }
}

fn text(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

fn string(value: &Value, label: &str) -> Result<String, String> {
    value
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("{label} is missing or empty"))
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => v.as_f64() != Some(0.0),
        Value::String(v) => !v.is_empty(),
        Value::Array(v) => !v.is_empty(),
        Value::Object(v) => !v.is_empty(),
    }
}

fn integer(value: &Value, label: &str, positive: bool) -> Result<u64, String> {
    let n = value
        .as_f64()
        .ok_or_else(|| format!("{label} must be numeric"))?;
    if !n.is_finite() || n < 0.0 || (positive && n == 0.0) {
        return Err(format!(
            "{label} must be {}",
            if positive {
                "positive"
            } else {
                "finite and nonnegative"
            }
        ));
    }
    if n.fract() != 0.0 {
        return Err(format!("{label} must be an integer"));
    }
    if let Some(n) = value.as_u64() {
        return Ok(n);
    }
    if n >= 18_446_744_073_709_551_616.0 {
        return Err(format!("{label} exceeds the integer counter range"));
    }
    Ok(n as u64)
}

fn counter(value: &Value, field: &str) -> Result<u64, String> {
    integer(&value[field], field, false)
}

fn sum(cells: &[Value], field: &str) -> Result<u64, String> {
    cells.iter().try_fold(0u64, |total, c| {
        total
            .checked_add(counter(c, field)?)
            .ok_or_else(|| format!("{field} sum overflows"))
    })
}

fn key(value: &Value) -> Result<Key, String> {
    Ok((
        string(&value["case"], "case")?,
        string(&value["workload"], "workload")?,
        string(&value["session_model"], "session_model")?,
        string(&value["channel_model"], "channel_model")?,
        integer(&value["concurrency"], "concurrency", true)?,
    ))
}

fn with_key(mut value: Value, key: &Key) -> Value {
    value["case"] = json!(key.0);
    value["workload"] = json!(key.1);
    value["session_model"] = json!(key.2);
    value["channel_model"] = json!(key.3);
    value["concurrency"] = json!(key.4);
    value
}

fn safe_relative(path: &Path) -> bool {
    !path.is_absolute()
        && !path.components().any(|c| {
            matches!(
                c,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
}

fn equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(a), Value::Number(b)) => match (a.as_u64(), b.as_u64()) {
            (Some(a), Some(b)) => a == b,
            _ => a.as_f64() == b.as_f64(),
        },
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| equal(a, b))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len() && a.iter().all(|(k, a)| b.get(k).is_some_and(|b| equal(a, b)))
        }
        _ => left == right,
    }
}

fn validate_cell(cell: &Value, label: &str) -> Result<(), String> {
    object(cell, label)?;
    for field in &KEY_FIELDS[..4] {
        string(&cell[*field], &format!("{label}.{field}"))?;
    }
    for field in ["concurrency", "requests", "elapsed_ns"] {
        integer(&cell[field], &format!("{label}.{field}"), true)?;
    }
    for field in [
        "repetition",
        "successes",
        "failures",
        "unsupported",
        "request_bytes",
        "response_bytes",
        "client_cpu_ns",
    ] {
        integer(&cell[field], &format!("{label}.{field}"), false)?;
    }
    let requests = counter(cell, "requests")?;
    if counter(cell, "concurrency")? > requests {
        return Err(format!("{label} has fewer requests than workers"));
    }
    let outcomes = counter(cell, "successes")?
        .checked_add(counter(cell, "failures")?)
        .and_then(|x| x.checked_add(counter(cell, "unsupported").ok()?));
    if outcomes != Some(requests) {
        return Err(format!("{label} outcome counts do not sum to requests"));
    }
    for field in [
        "latencies_ns",
        "first_message_latencies_ns",
        "first_arrow_latencies_ns",
    ] {
        if field == "first_arrow_latencies_ns" && cell.get(field).is_none() {
            continue;
        }
        let values = array(&cell[field], &format!("{label}.{field}"))?;
        for value in values {
            integer(value, &format!("{label}.{field}"), true)?;
        }
        if field == "latencies_ns" && values.len() as u64 != requests {
            return Err(format!("{label} requests do not have one latency each"));
        }
        if field != "latencies_ns" && values.len() as u64 > requests {
            return Err(format!(
                "{label} has more {} samples than requests",
                if field == "first_arrow_latencies_ns" {
                    "first-Arrow"
                } else {
                    "first-message"
                }
            ));
        }
    }
    let statuses = object(&cell["status_counts"], &format!("{label}.status_counts"))?;
    let statuses = statuses.values().try_fold(0u64, |total, n| {
        total
            .checked_add(integer(n, &format!("{label}.status_counts"), false)?)
            .ok_or_else(|| format!("{label} status counts overflow"))
    })?;
    if statuses != requests {
        return Err(format!("{label} status counts do not sum to requests"));
    }
    for field in ["validation_errors", "cleanup_errors"] {
        array(&cell[field], &format!("{label}.{field}"))?;
    }
    for field in ["warmup_validation_errors", "warmup_cleanup_errors"] {
        optional_array(cell, field, label)?;
    }
    for field in ["latency_window", "throughput_window"] {
        string(
            &cell[field],
            &format!("{label} timing-window definition {field}"),
        )?;
    }
    for field in [
        "validation_cleanup_ns",
        "preparation_ns",
        "full_round_elapsed_ns",
    ] {
        if let Some(value) = cell.get(field) {
            integer(value, &format!("{label}.{field}"), false)?;
        }
    }
    if cell.get("full_round_elapsed_ns").is_some() {
        let full = counter(cell, "full_round_elapsed_ns")?;
        if full < counter(cell, "elapsed_ns")? {
            return Err(format!(
                "{label} full round is shorter than its primary window"
            ));
        }
        if cell.get("preparation_ns").is_some() && cell.get("validation_cleanup_ns").is_some() {
            let components = counter(cell, "elapsed_ns")?
                .checked_add(counter(cell, "preparation_ns")?)
                .and_then(|x| x.checked_add(counter(cell, "validation_cleanup_ns").ok()?));
            if components != Some(full) {
                return Err(format!("{label} full-round component times do not sum"));
            }
        }
    }
    Ok(())
}

fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    Some(if values.len().is_multiple_of(2) {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    })
}

fn latencies(cells: &[Value], field: &str) -> Vec<f64> {
    cells
        .iter()
        .filter_map(|c| c[field].as_array())
        .flatten()
        .filter_map(Value::as_f64)
        .collect()
}

fn summarize_cells(cells: &[Value]) -> Result<Value, String> {
    let mut latency = latencies(cells, "latencies_ns");
    let median_ms = median(&mut latency).ok_or("cell group has no latency samples")? / 1e6;
    let rank =
        |q: f64| latency[((q * latency.len() as f64).ceil() as usize).saturating_sub(1)] / 1e6;
    let elapsed = sum(cells, "elapsed_ns")?;
    let requests = sum(cells, "requests")?;
    let response_bytes = sum(cells, "response_bytes")?;
    let cpu = sum(cells, "client_cpu_ns")?;
    let available_sum = |field: &str| -> Result<Option<u64>, String> {
        if cells.iter().all(|c| c.get(field).is_some()) {
            Ok(Some(sum(cells, field)?))
        } else {
            Ok(None)
        }
    };
    let full_elapsed = available_sum("full_round_elapsed_ns")?;
    let mut sorted = cells.iter().collect::<Vec<_>>();
    sorted.sort_by_key(|c| counter(c, "repetition").unwrap_or(0));
    let rounds = sorted.into_iter().map(|c| json!({"repetition":c["repetition"], "requests":c["requests"], "elapsed_ns":c["elapsed_ns"], "median_ms":median(&mut latencies(std::slice::from_ref(c), "latencies_ns")).unwrap_or(0.0)/1e6})).collect::<Vec<_>>();
    Ok(
        json!({"repetitions":cells.len(), "requests":requests, "elapsed_ns":elapsed,
        "median_ms":median_ms, "p95_ms":rank(0.95), "p99_ms":rank(0.99),
        "first_message_median_ms":median(&mut latencies(cells, "first_message_latencies_ns")).map(|n| n/1e6),
        "first_arrow_median_ms":median(&mut latencies(cells, "first_arrow_latencies_ns")).map(|n| n/1e6),
        "requests_per_s":requests as f64*1e9/elapsed as f64,
        "completed_round_requests_per_s":full_elapsed.map(|n| requests as f64*1e9/n as f64),
        "full_round_elapsed_ns":full_elapsed, "preparation_ns":available_sum("preparation_ns")?, "validation_cleanup_ns":available_sum("validation_cleanup_ns")?,
        "response_mib_per_s":if response_bytes == 0 { None } else { Some(response_bytes as f64*1e9/elapsed as f64/1_048_576.0) },
        "request_bytes":sum(cells,"request_bytes")?, "response_bytes":response_bytes, "client_cpu_ns":cpu, "client_cpu_wall_ratio":cpu as f64/elapsed as f64,
        "latency_window":cells[0]["latency_window"], "throughput_window":cells[0]["throughput_window"],
        "byte_accounting":cells[0].get("byte_accounting").cloned().unwrap_or_else(|| json!("protobuf encoded bytes, excluding gRPC/HTTP2 framing")), "rounds":rounds}),
    )
}

fn coverage_records(value: &Value) -> Result<&[Value], String> {
    if value.is_object() {
        match value.get("records").or_else(|| value.get("checks")) {
            Some(records) => array(records, "coverage records"),
            None => Ok(&[]),
        }
    } else if value.is_array() {
        array(value, "coverage records")
    } else {
        Ok(&[])
    }
}

fn normalized_settings(settings: &Value) -> Value {
    let mut settings = settings.clone();
    if let Some(settings) = settings.as_object_mut() {
        for key in ["endpoint", "output", "remote", "mode", "started_at"] {
            settings.remove(key);
        }
    }
    settings
}

fn validate_requested_grid(client: &Value, label: &str) -> Result<(), String> {
    let settings = &client["settings"];
    let coverage_only = settings["coverage_only"]
        .as_bool()
        .ok_or_else(|| format!("{label} lacks explicit coverage_only settings"))?;
    let capabilities = array(&client["capabilities"], &format!("{label} capabilities"))?;
    let cells = optional_array(client, "cells", label)?;
    let skipped = optional_array(client, "skipped_cells", label)?;
    if coverage_only {
        return if cells.is_empty() && skipped.is_empty() && capabilities.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "{label} coverage-only run contains timed work or capabilities"
            ))
        };
    }
    if capabilities.is_empty() {
        return Err(format!("{label} timed run has no capability records"));
    }
    let repetitions = integer(
        &settings["repetitions"],
        &format!("{label}.settings.repetitions"),
        true,
    )?;
    let requests = integer(
        &settings["requests"],
        &format!("{label}.settings.requests"),
        true,
    )?;
    let concurrency = array(
        &settings["concurrency"],
        &format!("{label} settings.concurrency"),
    )?;
    if concurrency.is_empty() {
        return Err(format!("{label} settings.concurrency is empty"));
    }
    let mut workers = BTreeSet::new();
    for value in concurrency {
        let value = integer(value, &format!("{label}.settings.concurrency"), true)?;
        if value > requests {
            return Err(format!("{label} requested concurrency exceeds requests"));
        }
        if !workers.insert(value) {
            return Err(format!("{label} settings.concurrency contains duplicates"));
        }
    }
    let mut models = Vec::new();
    for field in ["session_models", "channel_models"] {
        let values = array(&settings[field], &format!("{label}.settings.{field}"))?;
        let mut model = BTreeSet::new();
        for value in values {
            let value = value
                .as_str()
                .filter(|s| ["shared", "isolated"].contains(s))
                .ok_or_else(|| format!("{label}.settings.{field} is invalid"))?;
            if !model.insert(value.to_owned()) {
                return Err(format!("{label}.settings.{field} contains duplicates"));
            }
        }
        if model.is_empty() {
            return Err(format!("{label}.settings.{field} is invalid"));
        }
        models.push(model);
    }
    let mut cases = BTreeMap::new();
    for capability in capabilities {
        object(capability, &format!("{label} capability"))?;
        let case = string(&capability["case"], &format!("{label} capability case"))?;
        let rpc = string(&capability["rpc"], &format!("{label} capability rpc"))?;
        let supported = capability["supported"]
            .as_bool()
            .ok_or_else(|| format!("{label} capability lacks explicit support status"))?;
        if cases.insert(case, (rpc, supported)).is_some() {
            return Err(format!("{label} contains duplicate capability cases"));
        }
    }
    // Count the Cartesian product without allocating one entry per requested
    // repetition. Corrupt settings must not make validation allocate that grid.
    let expected_count = [
        cases.values().filter(|(_, supported)| *supported).count() as u128,
        workers.len() as u128,
        models[0].len() as u128,
        models[1].len() as u128,
        repetitions as u128,
    ]
    .into_iter()
    .try_fold(1u128, |n, factor| n.checked_mul(factor))
    .ok_or("requested grid size overflows")?;
    let mut actual: BTreeSet<CellIdentity> = BTreeSet::new();
    for cell in cells {
        if counter(cell, "requests")? != requests {
            return Err(format!(
                "{label} cell requests differ from requested settings"
            ));
        }
        if !actual.insert((key(cell)?, counter(cell, "repetition")?)) {
            return Err(format!(
                "{label} contains duplicate cell/repetition identities"
            ));
        }
    }
    for skip in skipped {
        object(skip, &format!("{label} skipped cell"))?;
        if !skip["reason"]
            .as_str()
            .is_some_and(|s| !s.trim().is_empty())
        {
            return Err(format!("{label} skipped cell lacks an explicit reason"));
        }
        let case = skip["case"]
            .as_str()
            .ok_or_else(|| format!("{label} skipped cell has an unknown capability case"))?;
        let (rpc, _) = cases
            .get(case)
            .ok_or_else(|| format!("{label} skipped cell has an unknown capability case"))?;
        let session = skip["session_model"]
            .as_str()
            .ok_or_else(|| format!("{label} skipped cell lacks session_model"))?;
        let channel = skip["channel_model"]
            .as_str()
            .ok_or_else(|| format!("{label} skipped cell lacks channel_model"))?;
        let worker = integer(
            &skip["concurrency"],
            &format!("{label}.skipped.concurrency"),
            true,
        )?;
        let repetition = integer(
            &skip["repetition"],
            &format!("{label}.skipped.repetition"),
            false,
        )?;
        if !actual.insert((
            (
                case.to_owned(),
                rpc.clone(),
                session.to_owned(),
                channel.to_owned(),
                worker,
            ),
            repetition,
        )) {
            return Err(format!(
                "{label} contains duplicate or measured-and-skipped cell identities"
            ));
        }
    }
    let matched = actual
        .iter()
        .filter(|((case, rpc, session, channel, worker), repetition)| {
            cases
                .get(case)
                .is_some_and(|(expected_rpc, supported)| *supported && rpc == expected_rpc)
                && workers.contains(worker)
                && models[0].contains(session)
                && models[1].contains(channel)
                && *repetition < repetitions
        })
        .count();
    let missing = expected_count - matched as u128;
    let unexpected = actual.len() - matched;
    if missing != 0 || unexpected != 0 {
        return Err(format!(
            "{label} requested cell grid differs: {missing} missing, {unexpected} unexpected"
        ));
    }
    Ok(())
}

fn validate_manifest_settings(
    settings: &Value,
    metadata: &Value,
    label: &str,
) -> Result<(), String> {
    for field in [
        "profile",
        "coverage_only",
        "requests",
        "warmups",
        "repetitions",
        "client_threads",
        "case_filter",
    ] {
        if let Some(expected) = metadata.get(field) {
            if !equal(&settings[field], expected) {
                return Err(format!(
                    "{label} client settings differ from manifest request: {field}"
                ));
            }
        }
    }
    for field in ["concurrency", "session_models", "channel_models"] {
        let Some(value) = metadata.get(field) else {
            continue;
        };
        let mut expected = if let Some(value) = value.as_str() {
            value.split(',').map(|s| json!(s)).collect::<Vec<_>>()
        } else {
            array(value, field)?.to_vec()
        };
        if field == "concurrency" {
            for value in &mut expected {
                if let Some(s) = value.as_str() {
                    *value = json!(s
                        .parse::<u64>()
                        .map_err(|e| format!("invalid manifest concurrency: {e}"))?);
                } else {
                    *value = json!(integer(value, "manifest concurrency", true)?);
                }
            }
        }
        if !equal(&settings[field], &json!(expected)) {
            return Err(format!(
                "{label} client settings differ from manifest request: {field}"
            ));
        }
    }
    if let Some(budget) = metadata.get("max_prepared_mib") {
        let budget = integer(budget, "max_prepared_mib", false)?
            .checked_mul(1 << 20)
            .ok_or("manifest preparation budget overflows")?;
        if integer(&settings["max_prepared_bytes"], "max_prepared_bytes", false)? != budget {
            return Err(format!(
                "{label} client settings differ from manifest request: max_prepared_bytes"
            ));
        }
    }
    Ok(())
}

fn load_fork(directory: &Path, record: &Value) -> Result<Value, String> {
    object(record, "fork record")?;
    let label = record["fork_id"].as_str().unwrap_or("unnamed fork");
    let raw = record["client_json"].as_str().unwrap_or("client.json");
    if record["clean_shutdown"] != true || record["client_exit_code"] != 0 {
        return Err(format!(
            "{label} failed or did not shut down cleanly; inspect raw {raw}"
        ));
    }
    if truthy(&record["error"]) {
        return Err(format!(
            "{label} failed: {}; inspect raw {raw}",
            text(&record["error"])
        ));
    }
    let relative = Path::new(raw);
    if !safe_relative(relative) {
        return Err(format!("{label} client path escapes the run directory"));
    }
    let path = directory.join(relative);
    let client = read_json(&path)?;
    object(&client, "client result")?;
    if client["passed"] != true {
        return Err(format!(
            "{label} client validation did not pass; inspect raw {raw}"
        ));
    }
    if ["incomplete", "in_progress", "error", "final_cleanup_errors"]
        .iter()
        .any(|k| truthy(&client[*k]))
    {
        return Err(format!(
            "{label} has incomplete work or final cleanup errors"
        ));
    }
    let settings = &client["settings"];
    if !settings.is_object() || settings["no_execution"] != true {
        return Err(format!("{label} lacks explicit execution-free settings"));
    }
    if settings["retries"] != 0 {
        return Err(format!("{label} does not explicitly disable retries"));
    }
    let profile = record["profile_kind"]
        .as_str()
        .filter(|s| ["fixture", "planning"].contains(s))
        .ok_or_else(|| format!("{label} client/server profile differs"))?;
    if client["mode"] != profile {
        return Err(format!("{label} client/server profile differs"));
    }
    let coverage = client.get("coverage").cloned().unwrap_or_else(|| json!({}));
    if coverage.is_object() && coverage["passed"] != true {
        return Err(format!("{label} coverage validation did not pass"));
    }
    let records = coverage_records(&coverage)?;
    if records.is_empty()
        || records
            .iter()
            .any(|r| !r.is_object() || r["passed"] != true)
    {
        return Err(format!("{label} has missing or failing coverage checks"));
    }
    let tested = records
        .iter()
        .filter(|r| r.get("tested").unwrap_or(&Value::Bool(true)) == &Value::Bool(true))
        .filter_map(|r| {
            r.get("route")
                .or_else(|| r.get("rpc"))
                .and_then(Value::as_str)
        })
        .collect::<BTreeSet<_>>();
    let expected = if profile == "fixture" {
        ROUTES.iter().copied().collect::<BTreeSet<_>>()
    } else {
        ["AnalyzePlan", "Config", "ReleaseSession"]
            .into_iter()
            .collect()
    };
    if !expected.is_subset(&tested) {
        return Err(format!(
            "{label} lacks required route probes: {}",
            expected
                .difference(&tested)
                .copied()
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if profile == "planning" && !tested.is_subset(&expected) {
        return Err(format!(
            "{label} planning profile probed a disallowed route"
        ));
    }
    let shutdown = &record["shutdown"];
    if shutdown["listener_closed"] != true {
        return Err(format!("{label} lacks closed-listener shutdown evidence"));
    }
    if profile == "fixture" {
        if shutdown["fixture_stats"]["query_executions"] != 0 {
            return Err(format!("{label} lacks its no-query fixture audit"));
        }
    } else if shutdown["spark_job_audit"]["jobs_started"] != 0 {
        return Err(format!("{label} lacks its zero-Spark-jobs audit"));
    }
    let mut grouped: BTreeMap<Key, Vec<Value>> = BTreeMap::new();
    let mut outcomes = Vec::new();
    let mut seen = BTreeSet::new();
    for (index, cell) in optional_array(&client, "cells", label)?.iter().enumerate() {
        validate_cell(cell, &format!("{label} cell {index}"))?;
        let key = key(cell)?;
        if !seen.insert((key.clone(), counter(cell, "repetition")?)) {
            return Err(format!(
                "{label} contains duplicate cell/repetition identities"
            ));
        }
        if [
            "failures",
            "unsupported",
            "validation_errors",
            "cleanup_errors",
            "warmup_validation_errors",
            "warmup_cleanup_errors",
        ]
        .iter()
        .any(|k| truthy(&cell[*k]))
        {
            let mut outcome = Map::new();
            for field in KEY_FIELDS.into_iter().chain([
                "repetition",
                "requests",
                "successes",
                "failures",
                "unsupported",
                "status_counts",
                "validation_errors",
                "cleanup_errors",
            ]) {
                outcome.insert(field.to_owned(), cell[field].clone());
            }
            for field in ["warmup_validation_errors", "warmup_cleanup_errors"] {
                outcome.insert(
                    field.to_owned(),
                    cell.get(field).cloned().unwrap_or_else(|| json!([])),
                );
            }
            outcomes.push(Value::Object(outcome));
        } else {
            grouped.entry(key).or_default().push(cell.clone());
        }
    }
    let mut groups = Vec::new();
    for (key, cells) in grouped {
        for field in ["latency_window", "throughput_window", "byte_accounting"] {
            if cells.iter().any(|c| c.get(field) != cells[0].get(field)) {
                return Err(format!(
                    "{label} mixes timing windows within one cell group"
                ));
            }
        }
        if cells.iter().any(|c| {
            c.get("full_round_elapsed_ns").is_some()
                != cells[0].get("full_round_elapsed_ns").is_some()
        }) {
            return Err(format!(
                "{label} mixes available and missing completed-round counters"
            ));
        }
        groups.push(with_key(summarize_cells(&cells)?, &key));
    }
    let skipped = optional_array(&client, "skipped_cells", label)?;
    validate_requested_grid(&client, label)?;
    let metadata = object(&client, "client result")?
        .iter()
        .filter(|(k, _)| !["cells", "coverage", "settings", "skipped_cells"].contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Map<_, _>>();
    let fork = integer(&record["fork"], "fork index", true)?;
    let target = string(&record["target"], "target")?;
    Ok(
        json!({"fork_id":label, "fork":fork, "target":target, "profile_kind":profile, "client_sha256":sha_file(&path)?, "settings":settings, "coverage":coverage, "groups":groups, "non_success_cells":outcomes, "skipped_cells":skipped, "client_metadata":metadata}),
    )
}

fn hash_matches(path: &Path, expected: &Value) -> bool {
    let Some(expected) = expected.as_str() else {
        return false;
    };
    path.is_file() && sha_file(path).is_ok_and(|actual| actual == expected)
}

fn reporter_hash() -> Result<String, String> {
    static HASH: OnceLock<Result<String, String>> = OnceLock::new();
    HASH.get_or_init(|| {
        std::env::current_exe()
            .map_err(|e| e.to_string())
            .and_then(|p| sha_file(&p))
    })
    .clone()
}

/// Read and validate a run without mutating it. Invalid forks remain diagnostics.
pub fn summarize(directory: &Path) -> Result<Value, String> {
    let manifest_path = directory.join("manifest.json");
    let manifest = read_json(&manifest_path)?;
    object(&manifest, "run manifest")?;
    let metadata = manifest
        .get("metadata")
        .cloned()
        .unwrap_or_else(|| json!({}));
    object(&metadata, "manifest metadata")?;
    let mut errors = Vec::<String>::new();
    let mut warnings = Vec::<String>::new();
    if manifest["complete"] != true {
        errors.push(format!(
            "run manifest is incomplete: {}",
            manifest
                .get("error")
                .map(text)
                .unwrap_or_else(|| "no complete marker".into())
        ));
    }
    let runs = match manifest["runs"].as_array() {
        Some(runs) if !runs.is_empty() => runs.as_slice(),
        _ => {
            errors.push("manifest contains no forks".into());
            &[]
        }
    };
    if let Some(artifacts) = metadata.get("artifacts") {
        for artifact in object(artifacts, "artifact records")?.values() {
            let snapshot = Path::new(
                artifact["snapshot"]
                    .as_str()
                    .ok_or("artifact snapshot is not a path")?,
            );
            let name = snapshot
                .file_name()
                .ok_or("artifact snapshot has no filename")?;
            let path = directory.join("artifacts").join(name);
            if !hash_matches(&path, &artifact["sha256"]) {
                errors.push(format!(
                    "frozen artifact hash differs: {}",
                    name.to_string_lossy()
                ));
            }
        }
    }
    for (field, legacy) in [
        ("runner_sha256", "runner.py"),
        ("reporter_sha256", "reporter.py"),
    ] {
        if let Some(expected) = metadata.get(field).filter(|v| truthy(v)) {
            let name = if metadata["orchestration"] == "rust" {
                "harness"
            } else {
                legacy
            };
            if !hash_matches(&directory.join("artifacts").join(name), expected) {
                errors.push(format!("frozen orchestration hash differs: {name}"));
            }
        }
    }
    if let Some(payloads) = metadata.get("payload_sha256") {
        for (name, expected) in object(payloads, "payload hashes")? {
            if !safe_relative(Path::new(name)) {
                errors.push("payload hash path escapes the run directory".into());
                continue;
            }
            if !hash_matches(&directory.join("payloads").join(name), expected) {
                errors.push(format!("frozen corpus hash differs: {name}"));
            }
        }
    }
    let corpus_path = directory.join("payloads/manifest.json");
    let corpus = if corpus_path.is_file() {
        Some(read_json(&corpus_path)?)
    } else {
        None
    };
    let mut forks = Vec::new();
    let mut seen = BTreeSet::new();
    let mut identities = BTreeSet::new();
    for record in runs {
        let result = (|| {
            let fork = load_fork(directory, record)?;
            let label = string(&fork["fork_id"], "fork_id")?;
            validate_manifest_settings(&fork["settings"], &metadata, &label)?;
            if seen.contains(&label) {
                return Err(format!("duplicate fork: {label}"));
            }
            let identity = (counter(&fork, "fork")?, string(&fork["target"], "target")?);
            if identities.contains(&identity) {
                return Err(format!("duplicate fork/target identity: {identity:?}"));
            }
            seen.insert(label);
            identities.insert(identity);
            Ok(fork)
        })();
        match result {
            Ok(fork) => {
                if fork["non_success_cells"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|c| {
                        [
                            "failures",
                            "validation_errors",
                            "cleanup_errors",
                            "warmup_validation_errors",
                            "warmup_cleanup_errors",
                        ]
                        .iter()
                        .any(|k| truthy(&c[*k]))
                    })
                {
                    errors.push(format!(
                        "{} contains failed cells; excluded from successful comparisons",
                        text(&fork["fork_id"])
                    ));
                }
                let skipped = fork["skipped_cells"].as_array().map_or(0, Vec::len);
                if skipped != 0 {
                    warnings.push(format!(
                        "{} skipped {skipped} cells; inspect reasons",
                        text(&fork["fork_id"])
                    ));
                }
                forks.push(fork);
            }
            Err(mut error) => {
                if let Some(raw) = record["client_json"].as_str().filter(|s| !s.is_empty()) {
                    if !error.contains(raw) {
                        error.push_str(&format!("; inspect raw {raw}"));
                    }
                }
                errors.push(error);
            }
        }
    }
    if metadata["forks_per_target"].is_number() && metadata["targets"].is_array() {
        let count = integer(&metadata["forks_per_target"], "forks_per_target", false)?;
        let mut expected = BTreeSet::new();
        for index in 1..=count {
            for target in array(&metadata["targets"], "targets")? {
                expected.insert((index, string(target, "target")?));
            }
        }
        let actual = forks
            .iter()
            .map(|f| (f["fork"].as_u64().unwrap_or(0), text(&f["target"])))
            .collect::<BTreeSet<_>>();
        if expected != actual {
            errors.push(
                "successful fork identities do not match requested targets/fork count".into(),
            );
        }
    }
    let mut comparisons: BTreeMap<(String, String, Key), Vec<Value>> = BTreeMap::new();
    let forkmap = forks
        .iter()
        .map(|f| ((f["fork"].as_u64().unwrap_or(0), text(&f["target"])), f))
        .collect::<BTreeMap<_, _>>();
    let indices = forks
        .iter()
        .filter_map(|f| f["fork"].as_u64())
        .collect::<BTreeSet<_>>();
    for (native, reference) in PAIR_TARGETS {
        for index in &indices {
            let (Some(left), Some(right)) = (
                forkmap.get(&(*index, native.into())),
                forkmap.get(&(*index, reference.into())),
            ) else {
                continue;
            };
            if !equal(
                &normalized_settings(&left["settings"]),
                &normalized_settings(&right["settings"]),
            ) {
                errors.push(format!(
                    "fork {index} {native}/{reference}: client settings differ"
                ));
                continue;
            }
            let left = *left;
            let right = *right;
            let lmap = array(&left["groups"], "groups")?
                .iter()
                .map(|g| key(g).map(|k| (k, g)))
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let rmap = array(&right["groups"], "groups")?
                .iter()
                .map(|g| key(g).map(|k| (k, g)))
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let lkeys = lmap.keys().cloned().collect::<BTreeSet<_>>();
            let rkeys = rmap.keys().cloned().collect::<BTreeSet<_>>();
            if lkeys != rkeys {
                warnings.push(format!("fork {index} {native}/{reference}: {} cell groups lack a successful counterpart; no ratios computed for them",lkeys.symmetric_difference(&rkeys).count()));
            }
            for key in lkeys.intersection(&rkeys) {
                let l = lmap[key];
                let r = rmap[key];
                if [
                    "requests",
                    "repetitions",
                    "latency_window",
                    "throughput_window",
                    "byte_accounting",
                ]
                .iter()
                .any(|field| !equal(&l[*field], &r[*field]))
                {
                    errors.push(format!(
                        "fork {index} {native}/{reference} {key:?}: sample counts/windows differ"
                    ));
                    continue;
                }
                let rounds = |group: &Value| -> Vec<Value> {
                    group["rounds"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|v| json!([v["repetition"], v["requests"]]))
                        .collect()
                };
                if !equal(&json!(rounds(l)), &json!(rounds(r))) {
                    errors.push(format!("fork {index} {native}/{reference} {key:?}: repetition identities/counts differ"));
                    continue;
                }
                let value = |g: &Value, field: &str| g[field].as_f64().unwrap_or(0.0);
                let mut pair = json!({"fork":index, "native_fork":left["fork_id"], "reference_fork":right["fork_id"], "latency_ratio":value(l,"median_ms")/value(r,"median_ms"), "throughput_ratio":value(l,"requests_per_s")/value(r,"requests_per_s"), "native_median_ms":l["median_ms"], "reference_median_ms":r["median_ms"], "native_requests_per_s":l["requests_per_s"], "reference_requests_per_s":r["requests_per_s"]});
                if !l["completed_round_requests_per_s"].is_null()
                    && !r["completed_round_requests_per_s"].is_null()
                {
                    pair["completed_round_throughput_ratio"] = json!(
                        value(l, "completed_round_requests_per_s")
                            / value(r, "completed_round_requests_per_s")
                    );
                    pair["native_completed_round_requests_per_s"] =
                        l["completed_round_requests_per_s"].clone();
                    pair["reference_completed_round_requests_per_s"] =
                        r["completed_round_requests_per_s"].clone();
                }
                comparisons
                    .entry((native.into(), reference.into(), key.clone()))
                    .or_default()
                    .push(pair);
            }
        }
    }
    let comparisons = comparisons.into_iter().map(|((native,reference,key),pairs)| {
        let mut item = with_key(json!({"native":native, "reference":reference, "pairs":pairs}),&key);
        for field in ["latency_ratio", "throughput_ratio", "completed_round_throughput_ratio"] {
            let values = item["pairs"].as_array().into_iter().flatten().filter_map(|p| p[field].as_f64()).collect::<Vec<_>>();
            if !values.is_empty() { item[field] = json!({"geometric_mean":(values.iter().map(|x|x.ln()).sum::<f64>()/values.len() as f64).exp(), "min":values.iter().copied().reduce(f64::min), "max":values.iter().copied().reduce(f64::max), "pairs":values.len()}); }
        }
        item
    }).collect::<Vec<_>>();
    if forks.len() < 4 {
        warnings.push("Few fresh forks: results are smoke/descriptive evidence, not a stable performance estimate.".into());
    }
    if forks
        .iter()
        .flat_map(|f| f["groups"].as_array().into_iter().flatten())
        .any(|g| {
            g["client_cpu_wall_ratio"]
                .as_f64()
                .is_some_and(|v| v >= 0.8)
        })
    {
        warnings.push("Some client CPU/wall ratios are >=0.8; measure client saturation before attributing a server ceiling.".into());
    }
    if comparisons.is_empty()
        && forks
            .iter()
            .any(|f| f["groups"].as_array().is_some_and(|v| !v.is_empty()))
    {
        warnings.push("No matching target pairs: per-fork measurements are available without cross-target ratios.".into());
    }
    let mut summary = json!({"schema_version":1, "generated_at":now_utc(), "manifest_sha256":sha_file(&manifest_path)?, "analysis_reporter_sha256":reporter_hash()?, "harness_kind":"rust", "harness_version":1, "metadata":metadata, "valid":errors.is_empty(), "errors":errors, "warnings":warnings, "forks":forks, "comparisons":comparisons,
        "methodology":{"independent_unit":"fresh server fork, matched within fork index", "latency":"descriptive per-fork pooled RPC latencies across repetitions of the same cell", "throughput":"successful requests / summed recorded primary-round elapsed time", "completed_round_throughput":"successful requests / summed preparation + primary RPC round + validation/cleanup time; excludes server startup, channel/session establishment, and warmups", "cross_fork_aggregation":"geometric mean and range of matched per-fork ratios", "unsupported":"never pooled with successful throughput or latency", "no_significance_claim":true}});
    if let Some(corpus) = corpus {
        summary["corpus"] = corpus;
    }
    Ok(summary)
}

fn esc(value: &Value) -> String {
    text(value).replace('|', "\\|").replace('\n', " ")
}
fn ratio(value: &Value) -> String {
    format!(
        "{:.3} [{:.3}, {:.3}]",
        value["geometric_mean"].as_f64().unwrap_or(0.0),
        value["min"].as_f64().unwrap_or(0.0),
        value["max"].as_f64().unwrap_or(0.0)
    )
}
fn display_optional(value: &Value, precision: usize) -> String {
    value
        .as_f64()
        .map(|v| format!("{v:.precision$}"))
        .unwrap_or_else(|| "—".into())
}

fn markdown(summary: &Value) -> String {
    let forks = summary["forks"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let comparisons = summary["comparisons"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let mut lines = vec!["# Execution-free Spark Connect concurrency benchmark".into(), String::new(),
        "This suite separates synthetic protocol handling from real-server planning. It uses the official Rust client's generated Spark Connect protocol; no throughput ratio crosses those two profiles.".into(), String::new(),
        format!("Status: **{}**. Fresh server forks: {}.",if summary["valid"] == true { "valid" } else { "INVALID / incomplete" },forks.len()),String::new()];
    if let Some(errors) = summary["errors"].as_array().filter(|v| !v.is_empty()) {
        lines.extend(["## Validation failures".into(), String::new()]);
        lines.extend(errors.iter().map(|e| format!("- {}", esc(e))));
        lines.push(String::new());
    }
    lines.extend(["## Matched concurrency cells".into(),String::new(),
        "Ratios are Rust/JVM within the same profile and cell: lower favors Rust for latency, higher favors Rust for request rate. Each pair uses separate fresh servers. The table shows geometric means and ranges across matched fork pairs; individual RPCs are not independent replications.".into(),String::new(),
        "| Profile comparison | Case / primary RPC | Concurrency | Sessions / channels | Pairs | Median latency ratio GM [range] | Primary request-rate ratio GM [range] | Completed-round rate ratio GM [range] |".into(),
        "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: |".into()]);
    for item in comparisons.iter().take(200) {
        lines.push(format!(
            "| {} / {} | {} / {} | {} | {} / {} | {} | {} | {} | {} |",
            esc(&item["native"]),
            esc(&item["reference"]),
            esc(&item["case"]),
            esc(&item["workload"]),
            text(&item["concurrency"]),
            esc(&item["session_model"]),
            esc(&item["channel_model"]),
            item["pairs"].as_array().map_or(0, Vec::len),
            ratio(&item["latency_ratio"]),
            ratio(&item["throughput_ratio"]),
            item.get("completed_round_throughput_ratio")
                .map(ratio)
                .unwrap_or_else(|| "—".into())
        ));
    }
    if comparisons.is_empty() {
        lines.push("| No matched successful cells | — | — | — | — | — | — | — |".into());
    }
    if comparisons.len() > 200 {
        lines.extend([String::new(),format!("Showing 200 of {} matched cell groups; `comparisons.csv` and `summary.json` contain every group.",comparisons.len())]);
    }
    lines.extend([String::new(),"## Per-fork measurements".into(),String::new(),
        "Only cells with every request successful, correct, and cleaned up appear here. p95/p99 are descriptive within each server fork. Primary rate uses the client's recorded primary-round window. Completed-round rate additionally includes request/lifecycle preparation and validation/cleanup; it excludes startup, channel/session establishment, and warmups. The report preserves each exact timing-window definition in `summary.json`.".into(),String::new(),
        "| Fork | Case / RPC | Concurrency | Sessions / channels | Reps / requests | Median / p95 / p99 ms | Primary requests/s | Completed-round requests/s | Response MiB/s | Client CPU:wall |".into(),
        "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |".into()]);
    let mut shown = 0;
    let mut total = 0;
    for fork in forks {
        for group in fork["groups"].as_array().into_iter().flatten() {
            total += 1;
            if shown >= 200 {
                continue;
            }
            shown += 1;
            lines.push(format!(
                "| {} | {} / {} | {} | {} / {} | {} / {} | {} / {} / {} | {} | {} | {} | {} |",
                esc(&fork["fork_id"]),
                esc(&group["case"]),
                esc(&group["workload"]),
                text(&group["concurrency"]),
                esc(&group["session_model"]),
                esc(&group["channel_model"]),
                text(&group["repetitions"]),
                text(&group["requests"]),
                display_optional(&group["median_ms"], 3),
                display_optional(&group["p95_ms"], 3),
                display_optional(&group["p99_ms"], 3),
                display_optional(&group["requests_per_s"], 1),
                display_optional(&group["completed_round_requests_per_s"], 1),
                display_optional(&group["response_mib_per_s"], 2),
                display_optional(&group["client_cpu_wall_ratio"], 3)
            ));
        }
    }
    if total > 200 {
        lines.extend([String::new(),format!("Showing 200 of {total} per-fork groups; `cells.csv` and `summary.json` contain every group.")]);
    }
    lines.extend([String::new(),"## Protocol coverage and unsupported outcomes".into(),String::new(),
        "Expected rejections and unavailable protocol versions are shown explicitly. A successful `UNIMPLEMENTED` capability probe verifies the rejection path; it does not mean that feature is implemented.".into(),String::new(),
        "| Fork | Route | Probe records | Statuses | Supported / tested |".into(),"| --- | --- | ---: | --- | --- |".into()]);
    for fork in forks {
        let mut grouped: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
        for record in coverage_records(&fork["coverage"]).unwrap_or(&[]) {
            grouped
                .entry(
                    record
                        .get("route")
                        .or_else(|| record.get("rpc"))
                        .map(text)
                        .unwrap_or_else(|| "unknown".into()),
                )
                .or_default()
                .push(record);
        }
        for (route, records) in grouped {
            let mut statuses: BTreeMap<String, usize> = BTreeMap::new();
            for record in &records {
                *statuses
                    .entry(
                        record
                            .get("status")
                            .or_else(|| record.get("outcome"))
                            .map(text)
                            .unwrap_or_else(|| "unknown".into()),
                    )
                    .or_default() += 1;
            }
            let labels = statuses
                .into_iter()
                .map(|(s, n)| format!("{}: {n}", s.replace('|', "\\|").replace('\n', " ")))
                .collect::<Vec<_>>()
                .join(", ");
            let supported = records.iter().filter(|r| r["supported"] == true).count();
            let tested = records
                .iter()
                .filter(|r| r.get("tested").unwrap_or(&Value::Bool(true)) == &Value::Bool(true))
                .count();
            lines.push(format!(
                "| {} | {} | {} | {labels} | {supported} / {tested} |",
                esc(&fork["fork_id"]),
                route.replace('|', "\\|").replace('\n', " "),
                records.len()
            ));
        }
        for item in fork["coverage"]["inventory"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|i| i["tested"] == false)
        {
            lines.push(format!(
                "| {} | {} | 0 | Not tested: {} | 0 / 0 |",
                esc(&fork["fork_id"]),
                item.get("route")
                    .map(esc)
                    .unwrap_or_else(|| "unknown".into()),
                item.get("not_tested_reason")
                    .map(esc)
                    .unwrap_or_else(|| "profile exclusion".into())
            ));
        }
    }
    let rejected: usize = forks
        .iter()
        .map(|f| f["non_success_cells"].as_array().map_or(0, Vec::len))
        .sum();
    lines.extend([String::new(),format!("Accepted forks contain **{rejected}** timed cells excluded for unsupported/failing outcomes. Their counts, statuses, and validation/cleanup errors remain in `summary.json`. Invalid forks are excluded entirely; validation failures name their raw `client.json` files for failure diagnostics."),String::new()]);
    for fork in forks {
        let count = fork["skipped_cells"].as_array().map_or(0, Vec::len);
        if count > 0 {
            lines.push(format!("- {}: {count} skipped cells; inspect `skipped_cells` for resource-budget/filter reasons.",esc(&fork["fork_id"])));
        }
    }
    lines.extend([String::new(),"## Scope and reproducibility".into(),String::new(),
        "Synthetic fixtures decode requests and return prebuilt Arrow responses without executing their plans. They measure those fixture implementations and runtime stacks, not the complete production frontends. Real-server planning calls avoid ExecutePlan and artifact upload; a Spark listener independently records zero job starts. These measurements exclude Spark task execution and worker-to-driver result production.".into(),String::new()]);
    lines.extend(
        summary["warnings"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|v| format!("- {}", esc(v))),
    );
    lines.extend(["- Concurrency is closed-loop with bounded in-flight requests. This is not an open-loop arrival-rate/SLO test.".into(),
        "- Changing session and channel sharing independently separates session serialization from HTTP/2 multiplexing effects.".into(),
        "- Startup, corpus generation, plan construction, validation, and lifecycle cleanup are outside the primary timing window; no application retries are enabled.".into(),
        "- Host load and per-process snapshots are retained. A shared desktop and small fork count do not establish a universal speedup.".into(),
        "- The manifest retains binary/jar/library/corpus hashes, JVM versions, commands, listener identity, shutdown, and no-execution audits.".into(),String::new(),"```json".into(),serde_json::to_string_pretty(&summary["metadata"]).unwrap_or_else(|_|"{}".into()),"```".into(),String::new()]);
    lines.join("\n")
}

fn csv_field(value: &Value) -> String {
    let value = if value.is_null() {
        String::new()
    } else {
        text(value)
    };
    if value.contains([',', '"', '\r', '\n']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value
    }
}

fn csv_row(fields: &[&str], row: &Value) -> String {
    fields
        .iter()
        .map(|field| csv_field(&row[*field]))
        .collect::<Vec<_>>()
        .join(",")
        + "\r\n"
}

fn write_csvs(directory: &Path, summary: &Value) -> Result<(), String> {
    let fields = [
        "fork_id",
        "target",
        "case",
        "workload",
        "session_model",
        "channel_model",
        "concurrency",
        "repetitions",
        "requests",
        "median_ms",
        "p95_ms",
        "p99_ms",
        "first_message_median_ms",
        "first_arrow_median_ms",
        "requests_per_s",
        "completed_round_requests_per_s",
        "response_mib_per_s",
        "request_bytes",
        "response_bytes",
        "client_cpu_wall_ratio",
        "preparation_ns",
        "validation_cleanup_ns",
        "full_round_elapsed_ns",
        "latency_window",
        "throughput_window",
        "byte_accounting",
    ];
    let mut cells = fields.join(",") + "\r\n";
    for fork in summary["forks"].as_array().into_iter().flatten() {
        for group in fork["groups"].as_array().into_iter().flatten() {
            let mut row = group.clone();
            row["fork_id"] = fork["fork_id"].clone();
            row["target"] = fork["target"].clone();
            cells.push_str(&csv_row(&fields, &row));
        }
    }
    let fields = [
        "native",
        "reference",
        "case",
        "workload",
        "session_model",
        "channel_model",
        "concurrency",
        "pairs",
        "latency_ratio_gm",
        "latency_ratio_min",
        "latency_ratio_max",
        "throughput_ratio_gm",
        "throughput_ratio_min",
        "throughput_ratio_max",
        "completed_round_throughput_ratio_gm",
        "completed_round_throughput_ratio_min",
        "completed_round_throughput_ratio_max",
    ];
    let mut comparisons = fields.join(",") + "\r\n";
    for group in summary["comparisons"].as_array().into_iter().flatten() {
        let mut row = group.clone();
        row["pairs"] = json!(group["pairs"].as_array().map_or(0, Vec::len));
        for field in [
            "latency_ratio",
            "throughput_ratio",
            "completed_round_throughput_ratio",
        ] {
            if group.get(field).is_some() {
                for (suffix, source) in [("gm", "geometric_mean"), ("min", "min"), ("max", "max")] {
                    row[format!("{field}_{suffix}")] = group[field][source].clone();
                }
            }
        }
        comparisons.push_str(&csv_row(&fields, &row));
    }
    fs::write(directory.join("cells.csv"), cells).map_err(|e| e.to_string())?;
    fs::write(directory.join("comparisons.csv"), comparisons).map_err(|e| e.to_string())
}

/// Always write diagnostics and all output files, including for invalid runs.
pub fn report(directory: &Path) -> Result<bool, String> {
    let summary = summarize(directory).unwrap_or_else(|error|json!({"schema_version":1, "valid":false, "errors":[error], "warnings":[], "metadata":{}, "forks":[], "comparisons":[]}));
    save(&directory.join("summary.json"), &summary)?;
    fs::write(directory.join("REPORT.md"), markdown(&summary)).map_err(|e| e.to_string())?;
    write_csvs(directory, &summary)?;
    Ok(summary["valid"] == true)
}

#[cfg(test)]
#[path = "report_tests.rs"]
mod tests;

//! Frozen, fresh-process comparisons of two Rust frontend / Spark JVM builds.
//!
//! The executable is the runner, reporter and load generator. Its frozen bytes,
//! the two server builds, corpus and policy are verified before and after every
//! child. Individual RPCs are never treated as independent server replications.

use crate::harness_util::{harness_identity, save, sha_file, SignalLease};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    path::{Component, Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

const METRICS: [(&str, &str); 5] = [
    ("median_latency", "median_ms"),
    ("p95_latency", "p95_ms"),
    ("p99_latency", "p99_ms"),
    ("throughput", "requests_per_s"),
    (
        "completed_round_throughput",
        "completed_round_requests_per_s",
    ),
];

static INTERRUPTED: AtomicBool = AtomicBool::new(false);

#[cfg(unix)]
extern "C" fn interrupted(_: libc::c_int) {
    INTERRUPTED.store(true, Ordering::SeqCst);
}

struct SignalGuard {
    #[cfg(unix)]
    previous: Vec<(libc::c_int, libc::sigaction)>,
    _lease: SignalLease,
}

impl SignalGuard {
    fn install() -> Result<Self, String> {
        let lease = SignalLease::acquire()?;
        INTERRUPTED.store(false, Ordering::SeqCst);
        let mut guard = Self {
            #[cfg(unix)]
            previous: Vec::new(),
            _lease: lease,
        };
        #[cfg(unix)]
        for signal in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP] {
            // POSIX sigaction's unused union/mask bytes must start zeroed.
            let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
            let mut previous: libc::sigaction = unsafe { std::mem::zeroed() };
            action.sa_sigaction = interrupted as *const () as usize;
            unsafe {
                libc::sigemptyset(&mut action.sa_mask);
                if libc::sigaction(signal, &action, &mut previous) != 0 {
                    return Err(format!(
                        "install comparison signal handler: {}",
                        std::io::Error::last_os_error()
                    ));
                }
            }
            guard.previous.push((signal, previous));
        }
        Ok(guard)
    }
}

impl Drop for SignalGuard {
    fn drop(&mut self) {
        #[cfg(unix)]
        for (signal, previous) in self.previous.iter().rev() {
            unsafe {
                libc::sigaction(*signal, previous, std::ptr::null_mut());
            }
        }
    }
}

struct ChildLease(Child);

impl Drop for ChildLease {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(Some(_))) {
            return;
        }
        // A suite child needs time to stop/reap its own server process group and
        // persist its failure evidence. Never immediately kill its coordinator.
        #[cfg(unix)]
        unsafe {
            libc::kill(self.0.id() as libc::pid_t, libc::SIGTERM);
        }
        #[cfg(not(unix))]
        let _ = self.0.kill();
        let deadline = Instant::now() + Duration::from_secs(40);
        while Instant::now() < deadline {
            if matches!(self.0.try_wait(), Ok(Some(_))) {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[derive(Clone, Debug)]
pub struct CompareOptions {
    pub output: PathBuf,
    pub harness_binary: PathBuf,
    pub binary: PathBuf,
    pub baseline_jar: PathBuf,
    pub baseline_library: PathBuf,
    pub candidate_jar: PathBuf,
    pub candidate_library: PathBuf,
    pub spark_home: PathBuf,
    pub java_home: PathBuf,
    pub payload_dir: Option<PathBuf>,
    pub policy: Option<PathBuf>,
    pub pairs: usize,
    pub start_order: String,
    pub control: bool,
    pub profile: String,
    pub concurrency: Vec<usize>,
    pub session_models: Vec<String>,
    pub channel_models: Vec<String>,
    pub requests: usize,
    pub warmups: usize,
    pub repetitions: usize,
    pub max_prepared_mib: usize,
    pub client_threads: usize,
    pub timeout_seconds: usize,
    pub client_timeout: usize,
    pub startup_timeout: usize,
    pub case: Option<String>,
}

impl CompareOptions {
    /// Reject invalid programmatic options before changing signal handlers or
    /// creating output, just as the command-line parser does.
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=20).contains(&self.pairs) || !matches!(self.start_order.as_str(), "AB" | "BA") {
            return Err("pairs must be 1..20 and start-order must be AB or BA".into());
        }
        if !matches!(self.profile.as_str(), "smoke" | "standard" | "stress") {
            return Err("profile must be smoke, standard or stress".into());
        }
        if self.concurrency.is_empty()
            || self.concurrency.iter().any(|n| !(1..=512).contains(n))
            || self.concurrency.iter().collect::<BTreeSet<_>>().len() != self.concurrency.len()
        {
            return Err("concurrency must contain distinct integers in 1..512".into());
        }
        for models in [&self.session_models, &self.channel_models] {
            if models.is_empty()
                || models.iter().any(|s| s != "shared" && s != "isolated")
                || models.iter().collect::<BTreeSet<_>>().len() != models.len()
            {
                return Err("sharing models must contain distinct shared/isolated values".into());
            }
        }
        if self.requests < *self.concurrency.iter().max().unwrap_or(&1)
            || self.requests > 100000
            || self.warmups > 100000
            || !(1..=100).contains(&self.repetitions)
            || !(1..=128).contains(&self.client_threads)
            || !(1..=600).contains(&self.timeout_seconds)
            || self.client_timeout == 0
            || self.startup_timeout == 0
            || self.max_prepared_mib == 0
        {
            return Err("invalid comparison counts, timeouts or memory budget".into());
        }
        self.max_prepared_mib
            .checked_mul(1024 * 1024)
            .ok_or("memory budget overflow")?;
        for (name, path) in [
            ("output", &self.output),
            ("harness_binary", &self.harness_binary),
            ("binary", &self.binary),
            ("baseline_jar", &self.baseline_jar),
            ("baseline_library", &self.baseline_library),
            ("candidate_jar", &self.candidate_jar),
            ("candidate_library", &self.candidate_library),
            ("spark_home", &self.spark_home),
            ("java_home", &self.java_home),
        ] {
            if path.as_os_str().is_empty() || !path.is_absolute() {
                return Err(format!("{name} must be an absolute path"));
            }
        }
        for (name, path) in [("payload_dir", &self.payload_dir), ("policy", &self.policy)] {
            if path
                .as_ref()
                .is_some_and(|p| p.as_os_str().is_empty() || !p.is_absolute())
            {
                return Err(format!("{name} must be an absolute path"));
            }
        }
        Ok(())
    }

    pub fn from_options(options: &BTreeMap<String, String>) -> Result<Self, String> {
        const ALLOWED: &[&str] = &[
            "--output",
            "--binary",
            "--baseline-jar",
            "--baseline-library",
            "--candidate-jar",
            "--candidate-library",
            "--spark-home",
            "--java-home",
            "--payload-dir",
            "--policy",
            "--pairs",
            "--start-order",
            "--control",
            "--profile",
            "--concurrency",
            "--session-models",
            "--channel-models",
            "--requests",
            "--warmups",
            "--repetitions",
            "--max-prepared-mib",
            "--client-threads",
            "--timeout-seconds",
            "--client-timeout",
            "--startup-timeout",
            "--case",
            "--help",
        ];
        for key in options.keys() {
            if !ALLOWED.contains(&key.as_str()) {
                return Err(format!("unknown comparison option {key}"));
            }
        }
        let text =
            |key: &str, default: &str| options.get(key).cloned().unwrap_or_else(|| default.into());
        let number = |key: &str, default: usize| -> Result<usize, String> {
            text(key, &default.to_string())
                .parse::<usize>()
                .map_err(|_| format!("{key} must be a nonnegative integer"))
        };
        let required = |key: &str| -> Result<PathBuf, String> {
            absolute(
                options
                    .get(key)
                    .ok_or_else(|| format!("{key} is required"))?,
            )
        };
        let installation = |key: &str, variable: &str| -> Result<PathBuf, String> {
            absolute(
                &options
                    .get(key)
                    .cloned()
                    .or_else(|| std::env::var(variable).ok().filter(|s| !s.is_empty()))
                    .ok_or_else(|| format!("{key} or {variable} must be set"))?,
            )
        };
        let pairs = number("--pairs", 3)?;
        let start_order = text("--start-order", "AB");
        let profile = text("--profile", "smoke");
        let concurrency = csv(&text("--concurrency", "1,8,32"), "concurrency")?
            .iter()
            .map(|value| {
                value
                    .parse::<usize>()
                    .ok()
                    .filter(|n| (1..=512).contains(n))
                    .ok_or_else(|| "concurrency must contain distinct integers in 1..512".into())
            })
            .collect::<Result<Vec<_>, String>>()?;
        if concurrency.iter().collect::<BTreeSet<_>>().len() != concurrency.len() {
            return Err("concurrency must contain distinct integers in 1..512".into());
        }
        let models = |key: &str| -> Result<Vec<String>, String> {
            let values = csv(&text(key, "shared"), key)?;
            if values.iter().any(|s| s != "shared" && s != "isolated") {
                return Err(format!("{key} supports shared and isolated"));
            }
            Ok(values)
        };
        let requests = number("--requests", 128)?;
        let warmups = number("--warmups", 64)?;
        let repetitions = number("--repetitions", 2)?;
        let max_prepared_mib = number("--max-prepared-mib", 60)?;
        let client_threads = number("--client-threads", 4)?;
        let timeout_seconds = number("--timeout-seconds", 30)?;
        let client_timeout = number("--client-timeout", 3600)?;
        let startup_timeout = number("--startup-timeout", 90)?;
        if !(1..=20).contains(&pairs) || !matches!(start_order.as_str(), "AB" | "BA") {
            return Err("pairs must be 1..20 and start-order must be AB or BA".into());
        }
        if !matches!(profile.as_str(), "smoke" | "standard" | "stress") {
            return Err("profile must be smoke, standard or stress".into());
        }
        if requests < *concurrency.iter().max().unwrap_or(&1)
            || requests > 100000
            || warmups > 100000
            || !(1..=100).contains(&repetitions)
            || !(1..=128).contains(&client_threads)
            || !(1..=600).contains(&timeout_seconds)
            || client_timeout == 0
            || startup_timeout == 0
            || max_prepared_mib == 0
        {
            return Err("invalid comparison counts or timeouts; requests must cover concurrency and remain <=100000, warmups <=100000, repetitions 1..100, client-threads 1..128, timeout-seconds 1..600, other timeouts/budget positive".into());
        }
        let harness_binary = std::env::current_exe().map_err(|e| e.to_string())?;
        let result = Self {
            output: required("--output")?,
            binary: options
                .get("--binary")
                .map(|p| absolute(p))
                .transpose()?
                .unwrap_or_else(|| harness_binary.clone()),
            harness_binary,
            baseline_jar: required("--baseline-jar")?,
            baseline_library: required("--baseline-library")?,
            candidate_jar: required("--candidate-jar")?,
            candidate_library: required("--candidate-library")?,
            spark_home: installation("--spark-home", "SPARK_HOME")?,
            java_home: installation("--java-home", "JAVA_HOME")?,
            payload_dir: options
                .get("--payload-dir")
                .map(|p| absolute(p))
                .transpose()?,
            policy: options.get("--policy").map(|p| absolute(p)).transpose()?,
            pairs,
            start_order,
            control: options
                .get("--control")
                .map(|s| s.parse::<bool>())
                .transpose()
                .map_err(|_| "--control must be a flag".to_string())?
                .unwrap_or(false),
            profile,
            concurrency,
            session_models: models("--session-models")?,
            channel_models: models("--channel-models")?,
            requests,
            warmups,
            repetitions,
            max_prepared_mib,
            client_threads,
            timeout_seconds,
            client_timeout,
            startup_timeout,
            case: options.get("--case").cloned(),
        };
        result.validate()?;
        Ok(result)
    }

    pub fn effective_matrix(&self) -> Value {
        json!({
            "profile": self.profile, "concurrency": self.concurrency,
            "requests": self.requests, "warmups": self.warmups,
            "repetitions": self.repetitions, "session_models": self.session_models,
            "channel_models": self.channel_models, "max_prepared_mib": self.max_prepared_mib,
            "case_filter": self.case,
        })
    }

    fn parameters(&self) -> Value {
        json!({
            "output": self.output, "harness_binary": self.harness_binary,
            "binary": self.binary, "baseline_jar": self.baseline_jar,
            "baseline_library": self.baseline_library, "candidate_jar": self.candidate_jar,
            "candidate_library": self.candidate_library, "spark_home": self.spark_home,
            "java_home": self.java_home, "payload_dir": self.payload_dir,
            "policy": self.policy, "pairs": self.pairs, "start_order": self.start_order,
            "control": self.control, "matrix": self.effective_matrix(),
            "client_threads": self.client_threads, "timeout_seconds": self.timeout_seconds,
            "client_timeout": self.client_timeout, "startup_timeout": self.startup_timeout,
        })
    }
}

fn absolute(value: &str) -> Result<PathBuf, String> {
    let path = if value == "~" || value.starts_with("~/") {
        let home = std::env::var_os("HOME").ok_or("cannot expand ~ without HOME")?;
        PathBuf::from(home).join(value.strip_prefix("~/").unwrap_or(""))
    } else {
        PathBuf::from(value)
    };
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()
            .map_err(|e| e.to_string())?
            .join(path))
    }
}

fn csv(value: &str, label: &str) -> Result<Vec<String>, String> {
    let values = value
        .split(',')
        .map(|s| s.trim().to_owned())
        .collect::<Vec<_>>();
    if values.is_empty()
        || values.iter().any(String::is_empty)
        || values.iter().collect::<BTreeSet<_>>().len() != values.len()
    {
        return Err(format!(
            "{label} must contain distinct comma-separated values"
        ));
    }
    Ok(values)
}

pub(crate) fn order(pair: usize, start: &str) -> [&'static str; 2] {
    if (start == "AB") == (pair % 2 == 1) {
        ["baseline", "candidate"]
    } else {
        ["candidate", "baseline"]
    }
}

fn positive(value: &Value, label: &str) -> Result<f64, String> {
    value
        .as_f64()
        .filter(|n| n.is_finite() && *n > 0.0)
        .ok_or_else(|| format!("{label} must be positive and finite"))
}

fn object<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>, String> {
    value
        .as_object()
        .ok_or_else(|| format!("{label} must be an object"))
}

fn array<'a>(value: &'a Value, label: &str) -> Result<&'a Vec<Value>, String> {
    value
        .as_array()
        .ok_or_else(|| format!("{label} must be an array"))
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CellKey {
    case: String,
    workload: String,
    session: String,
    channel: String,
    concurrency: u64,
}

impl CellKey {
    fn from_value(value: &Value) -> Result<Self, String> {
        let string = |field: &str| {
            value[field]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("cell {field} must be a string"))
        };
        Ok(Self {
            case: string("case")?,
            workload: string("workload")?,
            session: string("session_model")?,
            channel: string("channel_model")?,
            concurrency: value["concurrency"]
                .as_u64()
                .filter(|n| *n > 0)
                .ok_or("cell concurrency must be a positive integer")?,
        })
    }

    fn value(&self) -> Value {
        json!({"case": self.case, "workload": self.workload, "session_model": self.session,
            "channel_model": self.channel, "concurrency": self.concurrency})
    }

    fn label(&self) -> String {
        format!(
            "{}/{}/{}/{}/{}",
            self.case, self.workload, self.session, self.channel, self.concurrency
        )
    }
}

fn group_invariants(group: &Value) -> Result<Value, String> {
    let mut value = Map::new();
    for field in [
        "requests",
        "repetitions",
        "latency_window",
        "throughput_window",
        "byte_accounting",
    ] {
        value.insert(
            field.into(),
            group
                .get(field)
                .ok_or_else(|| format!("missing cell {field}"))?
                .clone(),
        );
    }
    let mut rounds = Vec::new();
    for row in array(&group["rounds"], "rounds")? {
        rounds.push(json!([
            row.get("repetition").ok_or("round has no repetition")?,
            row.get("requests").ok_or("round has no request count")?,
        ]));
    }
    value.insert("rounds".into(), Value::Array(rounds));
    Ok(Value::Object(value))
}

/// Each fresh pair, rather than each RPC, is one independent comparison unit.
pub(crate) fn aggregate(samples: &[Value], pairs: usize) -> Result<Vec<Value>, String> {
    if pairs == 0 {
        return Err("pairs must be a positive integer".into());
    }
    let mut indexed = BTreeMap::new();
    for sample in samples {
        let pair = sample["pair"]
            .as_u64()
            .filter(|n| *n > 0)
            .and_then(|n| usize::try_from(n).ok())
            .ok_or("pair identity must be positive and integral")?;
        let variant = sample["variant"]
            .as_str()
            .ok_or("variant identity must be a string")?;
        if !matches!(variant, "baseline" | "candidate")
            || indexed.contains_key(&(pair, variant.to_owned()))
        {
            return Err("duplicate or unknown variant identity".into());
        }
        let summary = &sample["summary"];
        let forks = array(&summary["forks"], "child forks")?;
        if summary["valid"] != true || forks.len() != 1 {
            return Err("child summary is invalid or does not contain one fresh fork".into());
        }
        let fork = &forks[0];
        if fork["target"] != "rust-jvm-planning" || fork["profile_kind"] != "planning" {
            return Err("child is not the Rust/JVM planning target".into());
        }
        if let Some(rows) = fork.get("non_success_cells") {
            if !array(rows, "non-success cells")?.is_empty() {
                return Err("child has unsupported or failed measured cells".into());
            }
        }
        indexed.insert((pair, variant.to_owned()), fork);
    }
    let expected = (1..=pairs)
        .flat_map(|pair| ["baseline", "candidate"].map(|v| (pair, v.to_owned())))
        .collect::<BTreeSet<_>>();
    if indexed.keys().cloned().collect::<BTreeSet<_>>() != expected {
        return Err("missing or unexpected fresh fork identities".into());
    }
    let mut fixed_settings = None;
    let mut fixed_skips = None;
    let mut fixed_grid = None;
    let mut invariants = BTreeMap::new();
    let mut grouped: BTreeMap<CellKey, Vec<Value>> = BTreeMap::new();
    for pair in 1..=pairs {
        let mut maps = Vec::new();
        for variant in ["baseline", "candidate"] {
            let fork = indexed[&(pair, variant.to_owned())];
            let mut settings = object(&fork["settings"], "measurement settings")?.clone();
            for field in ["endpoint", "output", "started_at"] {
                settings.remove(field);
            }
            if fixed_settings.get_or_insert_with(|| settings.clone()) != &settings {
                return Err("measurement settings differ across variants or pairs".into());
            }
            let mut skips = match fork.get("skipped_cells") {
                Some(value) => array(value, "skipped cells")?
                    .iter()
                    .map(Value::to_string)
                    .collect(),
                None => Vec::new(),
            };
            skips.sort();
            if fixed_skips.get_or_insert_with(|| skips.clone()) != &skips {
                return Err("skipped cell grids or reasons differ".into());
            }
            let groups = array(&fork["groups"], "child groups")?;
            let mut map = BTreeMap::new();
            for group in groups {
                let key = CellKey::from_value(group)?;
                if map.insert(key.clone(), group).is_some() {
                    return Err("empty or duplicated child cell grid".into());
                }
                let current = group_invariants(group)?;
                if invariants.entry(key).or_insert_with(|| current.clone()) != &current {
                    return Err(
                        "cell counts, repetition identities or timing windows differ".into(),
                    );
                }
            }
            if map.is_empty() {
                return Err("empty or duplicated child cell grid".into());
            }
            let grid = map.keys().cloned().collect::<BTreeSet<_>>();
            if fixed_grid.get_or_insert_with(|| grid.clone()) != &grid {
                return Err("successful cell grids differ across variants or pairs".into());
            }
            maps.push(map);
        }
        for key in fixed_grid.as_ref().ok_or("missing measured grid")? {
            let mut values = Map::new();
            for (index, variant) in ["baseline", "candidate"].iter().enumerate() {
                let group = maps[index][key];
                let mut metrics = Map::new();
                for (metric, field) in METRICS {
                    metrics.insert(metric.into(), json!(positive(&group[field], field)?));
                }
                let cpu = positive(&group["client_cpu_ns"], "client_cpu_ns")?
                    / positive(&group["requests"], "requests")?;
                metrics.insert("client_cpu_per_request".into(), json!(cpu));
                values.insert((*variant).into(), Value::Object(metrics));
            }
            let mut ratios = Map::new();
            for (metric, baseline) in object(&values["baseline"], "baseline metrics")? {
                let ratio =
                    positive(&values["candidate"][metric], metric)? / positive(baseline, metric)?;
                positive(&json!(ratio), "candidate/baseline ratio")?;
                ratios.insert(metric.clone(), json!(ratio));
            }
            values.insert("pair".into(), json!(pair));
            values.insert("ratios".into(), Value::Object(ratios));
            grouped
                .entry(key.clone())
                .or_default()
                .push(Value::Object(values));
        }
    }
    grouped
        .into_iter()
        .map(|(key, pairs)| {
            let mut ratios = Map::new();
            for metric in object(&pairs[0]["ratios"], "paired ratios")?.keys() {
                let values = pairs
                    .iter()
                    .map(|p| positive(&p["ratios"][metric], metric))
                    .collect::<Result<Vec<_>, _>>()?;
                let geometric_mean =
                    (values.iter().map(|n| n.ln()).sum::<f64>() / values.len() as f64).exp();
                positive(&json!(geometric_mean), "geometric mean ratio")?;
                ratios.insert(
                    metric.clone(),
                    json!({"geometric_mean": geometric_mean,
                "min": values.iter().copied().fold(f64::INFINITY, f64::min),
                "max": values.iter().copied().fold(f64::NEG_INFINITY, f64::max)}),
                );
            }
            let mut result = object(&key.value(), "cell identity")?.clone();
            result.insert("pairs".into(), Value::Array(pairs));
            result.insert("ratios".into(), Value::Object(ratios));
            Ok(Value::Object(result))
        })
        .collect()
}

pub(crate) fn gate(
    cells: &[Value],
    policy: Option<&Value>,
    control: bool,
) -> Result<Value, String> {
    if cells.is_empty() {
        return Err("no measured cells to evaluate".into());
    }
    let Some(policy) = policy else {
        return Ok(json!({"status": "descriptive", "passed": null, "failures": []}));
    };
    let mut bounds = vec![
        ("median_latency", "max_median_latency_ratio", false),
        ("throughput", "min_throughput_ratio", true),
        ("p95_latency", "max_p95_latency_ratio", false),
        (
            "client_cpu_per_request",
            "max_client_cpu_per_request_ratio",
            false,
        ),
    ];
    if policy.get("min_completed_round_throughput_ratio").is_some() {
        bounds.push((
            "completed_round_throughput",
            "min_completed_round_throughput_ratio",
            true,
        ));
    }
    for (_, bound, _) in &bounds {
        positive(&policy[*bound], bound)?;
    }
    let threshold = positive(
        &policy["min_target_throughput_ratio"],
        "min_target_throughput_ratio",
    )?;
    let minimum = positive(&policy["min_improved_targets"], "min_improved_targets")?;
    if minimum.fract() != 0.0 || threshold <= 1.0 {
        return Err(
            "target improvement count must be integral and throughput ratio must exceed one".into(),
        );
    }
    let targets = array(&policy["targets"], "policy targets")?;
    if targets.is_empty() {
        return Err("policy requires declared targets".into());
    }
    let mut required = BTreeSet::new();
    for target in targets {
        let case = target["case"]
            .as_str()
            .ok_or("target requires case and concurrency list")?;
        let workers = array(&target["concurrency"], "target concurrency")?;
        if workers.is_empty() {
            return Err("target requires case and concurrency list".into());
        }
        for worker in workers {
            let worker = worker
                .as_u64()
                .filter(|n| *n > 0)
                .ok_or("target concurrency must be a positive integer")?;
            required.insert((case.to_owned(), worker));
        }
    }
    let keys = cells
        .iter()
        .map(CellKey::from_value)
        .collect::<Result<Vec<_>, _>>()?;
    let available = keys
        .iter()
        .map(|key| (key.case.clone(), key.concurrency))
        .collect::<BTreeSet<_>>();
    if !required.is_subset(&available) {
        return Err("policy target is missing from the measured grid".into());
    }
    let all_pairs = policy
        .get("require_all_pairs_target_improve")
        .map(|v| {
            v.as_bool()
                .ok_or("require_all_pairs_target_improve must be boolean")
        })
        .transpose()?
        .unwrap_or(false);
    let mut failures = Vec::new();
    let mut improved_cells = Vec::new();
    let mut target_results: BTreeMap<(String, u64), Vec<bool>> = BTreeMap::new();
    for (cell, key) in cells.iter().zip(keys) {
        let ratio = |metric: &str| positive(&cell["ratios"][metric]["geometric_mean"], metric);
        for (metric, bound, lower) in &bounds {
            let value = ratio(metric)?;
            let limit = positive(&policy[*bound], bound)?;
            if if *lower { value < limit } else { value > limit } {
                failures.push(
                    json!({"cell": key.label(), "metric": metric, "ratio": value, "bound": limit}),
                );
            }
        }
        let target_key = (key.case.clone(), key.concurrency);
        if required.contains(&target_key) {
            let consistent = if all_pairs {
                let pairs = array(&cell["pairs"], "target pairs")?;
                if pairs.is_empty() {
                    return Err("target has no paired samples".into());
                }
                let mut consistent = true;
                for pair in pairs {
                    consistent &= positive(&pair["ratios"]["throughput"], "pair throughput ratio")?
                        > 1.0
                        && positive(
                            &pair["ratios"]["median_latency"],
                            "pair median latency ratio",
                        )? <= 1.0;
                }
                consistent
            } else {
                true
            };
            let qualifies =
                ratio("throughput")? >= threshold && ratio("median_latency")? <= 1.0 && consistent;
            target_results
                .entry(target_key)
                .or_default()
                .push(qualifies);
            if qualifies {
                improved_cells.push(key.label());
            }
        }
    }
    let improved = target_results
        .into_iter()
        .filter(|(_, values)| values.iter().all(|v| *v))
        .map(|((case, concurrency), _)| json!({"case": case, "concurrency": concurrency}))
        .collect::<Vec<_>>();
    if !control && (improved.len() as f64) < minimum {
        failures.push(json!({"reason": "too few improved target cells", "required": minimum, "actual": improved.len()}));
    }
    let passed = failures.is_empty();
    Ok(
        json!({"status": if passed { if control { "noise_control_pass" } else { "performance_candidate" } } else { "rejected" },
        "passed": passed, "improved_targets": improved, "improved_target_cells": improved_cells, "failures": failures}),
    )
}

pub(crate) fn validate_matrix(policy: Option<&Value>, matrix: &Value) -> Result<(), String> {
    let Some(expected) = policy.and_then(|p| p.get("matrix")) else {
        return Ok(());
    };
    let expected = object(expected, "policy matrix")?;
    if expected.is_empty() {
        return Err("policy matrix must be a nonempty object".into());
    }
    if expected
        .iter()
        .any(|(key, value)| matrix.get(key) != Some(value))
    {
        return Err("effective CLI matrix differs from frozen policy".into());
    }
    if matrix.get("case_filter").unwrap_or(&Value::Null)
        != expected.get("case_filter").unwrap_or(&Value::Null)
    {
        return Err("case filter differs from frozen policy".into());
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct FrozenInput {
    source: PathBuf,
    snapshot: PathBuf,
    sha256: String,
}

pub(crate) fn verify(inputs: &[FrozenInput]) -> Result<(), String> {
    for entry in inputs {
        for path in [&entry.source, &entry.snapshot] {
            if sha_file(path)? != entry.sha256 {
                return Err(format!("frozen input changed: {}", path.display()));
            }
        }
    }
    Ok(())
}

pub(crate) fn freeze(
    source: &Path,
    destination: &Path,
    inputs: &mut Vec<FrozenInput>,
) -> Result<PathBuf, String> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    fs::copy(source, destination).map_err(|e| format!("freeze {}: {e}", source.display()))?;
    let sha256 = sha_file(destination)?;
    if sha_file(source)? != sha256 {
        return Err(format!(
            "input changed while freezing: {}",
            source.display()
        ));
    }
    inputs.push(FrozenInput {
        source: source.to_path_buf(),
        snapshot: destination.to_path_buf(),
        sha256,
    });
    Ok(destination.to_path_buf())
}

fn read_json(path: &Path) -> Result<Value, String> {
    serde_json::from_slice(&fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?)
        .map_err(|e| format!("{}: {e}", path.display()))
}

fn path_text(path: &Path) -> Result<String, String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("path is not valid UTF-8: {}", path.display()))
}

fn basename(path: &Path) -> Result<&std::ffi::OsStr, String> {
    path.file_name()
        .ok_or_else(|| format!("path has no filename: {}", path.display()))
}

fn copy_payloads(source: &Path, destination: &Path) -> Result<(), String> {
    let manifest = read_json(&source.join("manifest.json"))?;
    let source_root = source.canonicalize().map_err(|e| e.to_string())?;
    fs::create_dir(destination).map_err(|e| e.to_string())?;
    let mut entries = array(&manifest["payloads"], "manifest payloads")?.clone();
    if let Some(cases) = manifest.get("cases") {
        entries.extend(array(cases, "manifest cases")?.iter().cloned());
    }
    for entry in entries {
        for field in [
            "schema_file",
            "arrow_file",
            "relation_file",
            "expected_schema_file",
        ] {
            let Some(name) = entry.get(field) else {
                continue;
            };
            let name = Path::new(name.as_str().ok_or("payload filename must be a string")?);
            if name.as_os_str().is_empty()
                || name
                    .components()
                    .any(|part| !matches!(part, Component::Normal(_)))
            {
                return Err("payload manifest path escapes its directory".into());
            }
            let input = source.join(name);
            if !input
                .canonicalize()
                .map_err(|e| e.to_string())?
                .starts_with(&source_root)
            {
                return Err("payload manifest symlink escapes its directory".into());
            }
            let output = destination.join(name);
            if let Some(parent) = output.parent() {
                fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            }
            fs::copy(input, output).map_err(|e| e.to_string())?;
        }
    }
    fs::copy(
        source.join("manifest.json"),
        destination.join("manifest.json"),
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn payload_files(root: &Path) -> Result<Vec<PathBuf>, String> {
    fn visit(root: &Path, paths: &mut Vec<PathBuf>) -> Result<(), String> {
        for entry in fs::read_dir(root).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            let kind = entry.file_type().map_err(|e| e.to_string())?;
            if kind.is_dir() {
                visit(&entry.path(), paths)?;
            } else if kind.is_file() {
                paths.push(entry.path());
            } else {
                return Err("frozen corpus may contain only regular files and directories".into());
            }
        }
        Ok(())
    }
    let mut paths = Vec::new();
    visit(root, &mut paths)?;
    paths.sort();
    Ok(paths)
}

fn execute(command: &[String], log: &Path, timeout: Option<Duration>) -> Result<i32, String> {
    if INTERRUPTED.load(Ordering::SeqCst) {
        return Err("comparison interrupted before starting child".into());
    }
    let first = command.first().ok_or("empty child command")?;
    let output = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .map_err(|e| e.to_string())?;
    let mut child = ChildLease(
        Command::new(first)
            .args(&command[1..])
            .stdin(Stdio::null())
            .stdout(Stdio::from(output.try_clone().map_err(|e| e.to_string())?))
            .stderr(Stdio::from(output))
            .spawn()
            .map_err(|e| format!("start {first}: {e}"))?,
    );
    let started = Instant::now();
    loop {
        if let Some(status) = child.0.try_wait().map_err(|e| e.to_string())? {
            return Ok(status.code().unwrap_or(-1));
        }
        if INTERRUPTED.load(Ordering::SeqCst) {
            return Err("comparison interrupted; child cleanup requested".into());
        }
        if timeout.is_some_and(|limit| started.elapsed() > limit) {
            return Err(format!("child timed out: {first}"));
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn markdown(result: &Value) -> String {
    let mut text = format!("# Rust/JVM engine build comparison\n\nStatus: **{}**. Ratios are Candidate/Baseline; lower latency/CPU and higher rates favor Candidate.\n\nThe Rust harness executable, load generator, corpus, policy and server artifacts are frozen. Each pair uses separate fresh JVMs in alternating order. Individual pairs remain in `compare.json`; no RPC samples are treated as independent server forks.\n\n", result["status"].as_str().unwrap_or("invalid"));
    if let Some(errors) = result["errors"].as_array() {
        for error in errors {
            text.push_str(&format!(
                "- {}\n",
                error.as_str().unwrap_or("comparison failure")
            ));
        }
    }
    text.push_str("\n| Case | Concurrency | Sessions/channels | Median latency | p95 latency | Primary rate | Completed-round rate | Client CPU/request |\n| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    if let Some(cells) = result["cells"].as_array() {
        for cell in cells {
            let field = |name: &str| {
                cell[name]
                    .as_str()
                    .unwrap_or("")
                    .replace('|', "\\|")
                    .replace('\n', " ")
            };
            text.push_str(&format!(
                "| {} | {} | {}/{} |",
                field("case"),
                cell["concurrency"],
                field("session_model"),
                field("channel_model")
            ));
            for metric in [
                "median_latency",
                "p95_latency",
                "throughput",
                "completed_round_throughput",
                "client_cpu_per_request",
            ] {
                text.push_str(&format!(
                    " {:.3} |",
                    cell["ratios"][metric]["geometric_mean"]
                        .as_f64()
                        .unwrap_or(0.0)
                ));
            }
            text.push('\n');
        }
    }
    text.push_str("\nGuard and target decisions, skipped cells and raw failures are retained. A control skips the improvement requirement and checks guard bounds. A performance_candidate is an exploratory local gate; it still requires independent confirmation and broader correctness/holdout checks.\n");
    text
}

fn save_result(output: &Path, result: &mut Value, inputs: &[FrozenInput]) -> Result<(), String> {
    result["inputs"] = serde_json::to_value(inputs).map_err(|e| e.to_string())?;
    save(&output.join("compare.json"), result)
}

/// Returns false for both rejected gates and invalid comparisons. Failure evidence
/// is written before returning; only an inability to create/save evidence is Err.
pub fn run(options: CompareOptions) -> Result<bool, String> {
    options.validate()?;
    let _signals = SignalGuard::install()?;
    run_with(options, execute)
}

pub(crate) fn run_with<F>(options: CompareOptions, mut execute: F) -> Result<bool, String>
where
    F: FnMut(&[String], &Path, Option<Duration>) -> Result<i32, String>,
{
    options.validate()?;
    if let Some(parent) = options.output.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    fs::create_dir(&options.output).map_err(|e| format!("create new comparison directory: {e}"))?;
    let mut result = json!({"schema_version": 1, "valid": false, "status": "invalid",
        "harness_kind": "rust", "harness_version": 1, "producer": harness_identity(),
        "control": options.control, "pairs_requested": options.pairs, "start_order": options.start_order,
        "parameters": options.parameters(), "matrix": options.effective_matrix(),
        "inputs": [], "runs": [], "cells": [], "errors": []});
    let mut inputs = Vec::new();
    let outcome = run_inner(&options, &mut result, &mut inputs, &mut execute);
    if let Err(error) = outcome {
        result["errors"].as_array_mut().unwrap().push(json!(error));
    }
    if let Err(error) = verify(&inputs) {
        result["valid"] = json!(false);
        result["status"] = json!("invalid");
        result["errors"].as_array_mut().unwrap().push(json!(error));
    }
    save_result(&options.output, &mut result, &inputs)?;
    fs::write(options.output.join("COMPARE.md"), markdown(&result)).map_err(|e| e.to_string())?;
    println!(
        "{}",
        json!({"valid": result["valid"], "status": result["status"],
        "comparison": options.output.join("compare.json"), "errors": result["errors"]})
    );
    Ok(result["valid"] == true && result["status"] != "rejected")
}

fn run_inner<F>(
    options: &CompareOptions,
    result: &mut Value,
    inputs: &mut Vec<FrozenInput>,
    execute: &mut F,
) -> Result<(), String>
where
    F: FnMut(&[String], &Path, Option<Duration>) -> Result<i32, String>,
{
    let harness = freeze(
        &options.harness_binary,
        &options
            .output
            .join("artifacts/harness")
            .join(basename(&options.harness_binary)?),
        inputs,
    )?;
    let harness_sha = sha_file(&harness)?;
    result["frozen_harness_sha256"] = json!(harness_sha);
    let binary = freeze(
        &options.binary,
        &options
            .output
            .join("artifacts/loadgen")
            .join(basename(&options.binary)?),
        inputs,
    )?;
    let mut artifacts = BTreeMap::new();
    for (variant, jar, library) in [
        ("baseline", &options.baseline_jar, &options.baseline_library),
        (
            "candidate",
            &options.candidate_jar,
            &options.candidate_library,
        ),
    ] {
        let mut saved = BTreeMap::new();
        for (kind, source) in [("jar", jar), ("library", library)] {
            let destination = options
                .output
                .join("artifacts")
                .join(variant)
                .join(kind)
                .join(basename(source)?);
            saved.insert(kind, freeze(source, &destination, inputs)?);
        }
        artifacts.insert(variant, saved);
    }
    if options.control {
        for kind in ["jar", "library"] {
            if sha_file(&artifacts["baseline"][kind])? != sha_file(&artifacts["candidate"][kind])? {
                return Err("noise control requires identical jar and native library bytes".into());
            }
        }
    }
    let policy = options
        .policy
        .as_ref()
        .map(|path| {
            let frozen = freeze(path, &options.output.join("artifacts/policy.json"), inputs)?;
            read_json(&frozen)
        })
        .transpose()?;
    result["policy"] = policy.clone().unwrap_or(Value::Null);
    validate_matrix(policy.as_ref(), &options.effective_matrix())?;
    let payloads = options.output.join("payloads");
    if let Some(source) = &options.payload_dir {
        copy_payloads(source, &payloads)?;
    } else {
        let command = vec![
            path_text(&binary)?,
            "export".into(),
            "--profile".into(),
            options.profile.clone(),
            "--output".into(),
            path_text(&payloads)?,
        ];
        if execute(
            &command,
            &options.output.join("export.log"),
            Some(Duration::from_secs(120)),
        )? != 0
        {
            return Err("corpus export failed; see export.log".into());
        }
    }
    for path in payload_files(&payloads)? {
        let name = path.strip_prefix(&payloads).map_err(|e| e.to_string())?;
        inputs.push(FrozenInput {
            source: options
                .payload_dir
                .as_ref()
                .map(|source| source.join(name))
                .unwrap_or_else(|| path.clone()),
            snapshot: path.clone(),
            sha256: sha_file(&path)?,
        });
    }
    let mut samples = Vec::new();
    for pair in 1..=options.pairs {
        for variant in order(pair, &options.start_order) {
            verify(inputs)?;
            let folder = options.output.join(format!("pair{pair}-{variant}"));
            let mut command = vec![
                path_text(&harness)?,
                "suite".into(),
                "--output".into(),
                path_text(&folder)?,
                "--targets".into(),
                "rust-jvm-planning".into(),
                "--forks".into(),
                "1".into(),
                "--binary".into(),
                path_text(&binary)?,
                "--jar".into(),
                path_text(&artifacts[variant]["jar"])?,
                "--library".into(),
                path_text(&artifacts[variant]["library"])?,
                "--payload-dir".into(),
                path_text(&payloads)?,
                "--spark-home".into(),
                path_text(&options.spark_home)?,
                "--java-home".into(),
                path_text(&options.java_home)?,
            ];
            for (key, value) in [
                ("profile", options.profile.clone()),
                (
                    "concurrency",
                    options
                        .concurrency
                        .iter()
                        .map(usize::to_string)
                        .collect::<Vec<_>>()
                        .join(","),
                ),
                ("session-models", options.session_models.join(",")),
                ("channel-models", options.channel_models.join(",")),
                ("requests", options.requests.to_string()),
                ("warmups", options.warmups.to_string()),
                ("repetitions", options.repetitions.to_string()),
                ("max-prepared-mib", options.max_prepared_mib.to_string()),
                ("client-threads", options.client_threads.to_string()),
                ("timeout-seconds", options.timeout_seconds.to_string()),
                ("client-timeout", options.client_timeout.to_string()),
                ("startup-timeout", options.startup_timeout.to_string()),
            ] {
                command.extend([format!("--{key}"), value]);
            }
            if let Some(case) = &options.case {
                command.extend(["--case".into(), case.clone()]);
            }
            let record =
                json!({"pair": pair, "variant": variant, "directory": folder, "command": command});
            let index = result["runs"].as_array().unwrap().len();
            result["runs"].as_array_mut().unwrap().push(record);
            save_result(&options.output, result, inputs)?;
            println!("Pair {pair}: {variant}");
            let log = options.output.join(format!("pair{pair}-{variant}.log"));
            let run_exit = execute(&command, &log, None)?;
            result["runs"][index]["run_exit_code"] = json!(run_exit);
            let report_command = vec![
                path_text(&harness)?,
                "report".into(),
                "--input".into(),
                path_text(&folder)?,
            ];
            let report_exit = execute(&report_command, &log, Some(Duration::from_secs(120)))?;
            result["runs"][index]["report_exit_code"] = json!(report_exit);
            verify(inputs)?;
            if run_exit != 0 || report_exit != 0 {
                return Err(format!(
                    "invalid child {pair}/{variant}; inspect {} and its log",
                    folder.display()
                ));
            }
            let summary = read_json(&folder.join("summary.json"))?;
            if summary["analysis_reporter_sha256"] != harness_sha {
                return Err("child summary was not produced by the frozen Rust reporter".into());
            }
            samples.push(json!({"pair": pair, "variant": variant, "summary": summary}));
        }
    }
    let cells = aggregate(&samples, options.pairs)?;
    result["cells"] = json!(cells);
    result["decision"] = gate(&cells, policy.as_ref(), options.control)?;
    result["status"] = result["decision"]["status"].clone();
    result["valid"] = json!(true);
    Ok(())
}

#[cfg(test)]
#[path = "compare_tests.rs"]
mod tests;

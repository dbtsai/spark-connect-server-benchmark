use prost::Message;
use serde_json::{json, Value};
use spark_connect_protocol_bench::{
    corpus,
    fixture::Fixture,
    grpc,
    load::{self, Settings},
};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

fn help() {
    println!(
        r#"Spark Connect protocol concurrency benchmark (pinned fork Rust client/protobufs)

suite --output DIRECTORY [--targets rust-fixture,jvm-fixture,jvm-planning,rust-jvm-planning]
    [--profile smoke|standard|stress] [--forks 2] [--concurrency 1,8,32]
    [--requests 64] [--warmups 16] [--repetitions 3] [--coverage-only]
    [--spark-home DIRECTORY] [--java-home DIRECTORY] [--binary FILE] [--jar FILE] [--library FILE]
report --input RUN_DIRECTORY
compare --output DIRECTORY --baseline-jar FILE --baseline-library FILE
    --candidate-jar FILE --candidate-library FILE [--policy FILE] [--pairs 3]
    [--start-order AB|BA] [--control] [--requests 128] [--warmups 64] [--repetitions 2]
api-audit [--output result.json] [--strict]

export --profile smoke|standard|stress --output DIRECTORY
serve-fixture --payload-dir DIRECTORY [--listen 127.0.0.1:0] [--threads 4]
run --endpoint http://127.0.0.1:PORT --mode fixture|planning --payload-dir DIRECTORY
    --output result.json [--profile smoke] [--concurrency 1,8,32]
    [--session-models shared,isolated] [--channel-models shared,isolated]
    [--requests 64] [--warmups 16] [--repetitions 3] [--client-threads 4]
    [--timeout-seconds 30] [--max-prepared-mib 256] [--case SUBSTRING] [--coverage-only]
codec --profile smoke|standard|stress --output result.json [--iterations 128]

The planning profile sends only AnalyzePlan, Config, and ReleaseSession.
The fixture profile additionally exercises synthetic prebuilt result streams and all RPC routes.
Plans and payloads are prepared before timing; no query engine executes a plan in either profile.
Each cell uses closed-loop workers; failed and unsupported calls are reported separately.
"#
    );
}
fn args() -> Result<(String, BTreeMap<String, String>), String> {
    let mut a = std::env::args().skip(1);
    let command = a.next().unwrap_or_else(|| "help".into());
    let mut options = BTreeMap::new();
    while let Some(key) = a.next() {
        if !key.starts_with("--") {
            return Err(format!("expected option, got {key}"));
        }
        let value = if matches!(
            key.as_str(),
            "--coverage-only" | "--help" | "--control" | "--strict"
        ) {
            "true".into()
        } else {
            a.next().ok_or_else(|| format!("missing value for {key}"))?
        };
        if options.insert(key.clone(), value).is_some() {
            return Err(format!("duplicate option {key}"));
        }
    }
    Ok((command, options))
}
fn get(options: &BTreeMap<String, String>, key: &str, default: &str) -> String {
    options.get(key).cloned().unwrap_or_else(|| default.into())
}
fn number(options: &BTreeMap<String, String>, key: &str, default: usize) -> Result<usize, String> {
    get(options, key, &default.to_string())
        .parse()
        .map_err(|_| format!("invalid integer for {key}"))
}
fn required(options: &BTreeMap<String, String>, key: &str) -> Result<String, String> {
    options
        .get(key)
        .cloned()
        .ok_or_else(|| format!("{key} is required"))
}
fn list(options: &BTreeMap<String, String>, key: &str, default: &str) -> Vec<String> {
    get(options, key, default)
        .split(',')
        .map(str::to_owned)
        .collect()
}
fn check_options(options: &BTreeMap<String, String>, allowed: &[&str]) -> Result<(), String> {
    for key in options.keys() {
        if !allowed.contains(&key.as_str()) && key != "--help" {
            return Err(format!("unknown option {key}"));
        }
    }
    Ok(())
}
fn write(path: &PathBuf, value: &Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    std::fs::write(
        path,
        serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())
}
fn codec(options: &BTreeMap<String, String>) -> Result<bool, String> {
    check_options(options, &["--profile", "--output", "--iterations"])?;
    let cases = corpus::build(&get(options, "--profile", "smoke"))?;
    let iterations = number(options, "--iterations", 128)?;
    if iterations == 0 || iterations > 100000 {
        return Err("iterations must be 1..100000".into());
    }
    let mut results = Vec::new();
    for case in cases {
        let request = spark_connect_proto::AnalyzePlanRequest {
            session_id: "00000000-0000-4000-8000-000000000001".into(),
            user_context: Some(spark_connect_proto::UserContext {
                user_id: "protocol-bench".into(),
                ..Default::default()
            }),
            analyze: Some(spark_connect_proto::analyze_plan_request::Analyze::Schema(
                spark_connect_proto::analyze_plan_request::Schema {
                    plan: Some(spark_connect_proto::Plan {
                        op_type: Some(spark_connect_proto::plan::OpType::Root(case.relation)),
                    }),
                },
            )),
            ..Default::default()
        };
        let bytes = request.encode_to_vec();
        let mut encoded = Vec::new();
        let mut decoded = Vec::new();
        for _ in 0..iterations {
            let start = Instant::now();
            let data = std::hint::black_box(&request).encode_to_vec();
            encoded.push(start.elapsed().as_nanos() as u64);
            std::hint::black_box(data);
            let start = Instant::now();
            let value = spark_connect_proto::AnalyzePlanRequest::decode(std::hint::black_box(
                bytes.as_slice(),
            ))
            .map_err(|e| e.to_string())?;
            decoded.push(start.elapsed().as_nanos() as u64);
            std::hint::black_box(value);
        }
        results.push(json!({"case":case.name,"request_bytes":bytes.len(),"iterations":iterations,"encode_latencies_ns":encoded,"decode_latencies_ns":decoded,"metadata":case.metadata}));
    }
    write(
        &PathBuf::from(required(options, "--output")?),
        &json!({"schema_version":1,"passed":true,"mode":"codec","profile":get(options,"--profile","smoke"),"cases":results,"note":"Client encode/decode allocation cost; no network or server, no baseline subtraction"}),
    )?;
    Ok(true)
}
async fn serve(options: &BTreeMap<String, String>) -> Result<bool, String> {
    check_options(options, &["--payload-dir", "--listen", "--threads"])?;
    let addr: SocketAddr = get(options, "--listen", "127.0.0.1:0")
        .parse()
        .map_err(|e| format!("invalid listen address: {e}"))?;
    if !addr.ip().is_loopback() {
        return Err("synthetic benchmark fixture must listen on loopback".into());
    }
    let fixture = Arc::new(Fixture::load(
        &PathBuf::from(required(options, "--payload-dir")?).join("manifest.json"),
    )?);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| e.to_string())?;
    println!(
        "PROTOCOL_FIXTURE_LISTENING {}",
        listener.local_addr().map_err(|e| e.to_string())?
    );
    let stop_fixture = fixture.clone();
    tonic::transport::Server::builder()
        .add_service(
            grpc::spark_connect_service_server::SparkConnectServiceServer::from_arc(
                fixture.clone(),
            )
            .max_decoding_message_size(64 * 1024 * 1024)
            .max_encoding_message_size(64 * 1024 * 1024),
        )
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            async move {
                #[cfg(unix)]
                {
                    let mut terminate =
                        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                            .expect("SIGTERM handler");
                    tokio::select! {_=tokio::signal::ctrl_c()=>{},_=terminate.recv()=>{}}
                }
                #[cfg(not(unix))]
                {
                    let _ = tokio::signal::ctrl_c().await;
                }
                stop_fixture.shutdown();
            },
        )
        .await
        .map_err(|e| e.to_string())?;
    println!("PROTOCOL_FIXTURE_STATS {}", fixture.stats());
    println!("PROTOCOL_FIXTURE_SHUTDOWN");
    Ok(true)
}
fn run() -> Result<bool, String> {
    let (command, options) = args()?;
    if matches!(command.as_str(), "help" | "--help" | "-h") || options.contains_key("--help") {
        help();
        return Ok(true);
    }
    if command == "suite" {
        spark_connect_protocol_bench::runner::run(
            spark_connect_protocol_bench::runner::RunOptions::from_options(&options)?,
        )?;
        return Ok(true);
    }
    if command == "report" {
        check_options(&options, &["--input"])?;
        let directory = PathBuf::from(required(&options, "--input")?);
        let valid = spark_connect_protocol_bench::report::report(&directory)?;
        println!(
            "{}",
            json!({"valid":valid,"report":directory.join("REPORT.md")})
        );
        return Ok(valid);
    }
    if command == "compare" {
        return spark_connect_protocol_bench::compare::run(
            spark_connect_protocol_bench::compare::CompareOptions::from_options(&options)?,
        );
    }
    if command == "api-audit" {
        check_options(&options, &["--output", "--strict"])?;
        let value = spark_connect_protocol_bench::api_audit::audit()?;
        if let Some(path) = options.get("--output") {
            spark_connect_protocol_bench::harness_util::save(&PathBuf::from(path), &value)?;
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&value).map_err(|e| e.to_string())?
        );
        return Ok(!options.contains_key("--strict") || value["has_client_gaps"] == false);
    }
    if command == "export" {
        check_options(&options, &["--profile", "--output"])?;
        let cases = corpus::build(&get(&options, "--profile", "smoke"))?;
        let manifest = corpus::export(&cases, &PathBuf::from(required(&options, "--output")?))?;
        println!(
            "{}",
            json!({"passed":true,"cases":cases.len(),"payloads":manifest["payloads"].as_array().map(Vec::len)})
        );
        return Ok(true);
    }
    if command == "codec" {
        return codec(&options);
    }
    let threads = number(
        &options,
        if command == "serve-fixture" {
            "--threads"
        } else {
            "--client-threads"
        },
        4,
    )?;
    if threads == 0 || threads > 128 {
        return Err("runtime threads must be 1..128".into());
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .thread_stack_size(8 * 1024 * 1024)
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    if command == "serve-fixture" {
        return runtime.block_on(serve(&options));
    }
    if command != "run" {
        return Err(format!("unknown command {command}"));
    }
    check_options(
        &options,
        &[
            "--endpoint",
            "--mode",
            "--payload-dir",
            "--output",
            "--profile",
            "--concurrency",
            "--session-models",
            "--channel-models",
            "--requests",
            "--warmups",
            "--repetitions",
            "--client-threads",
            "--timeout-seconds",
            "--max-prepared-mib",
            "--case",
            "--coverage-only",
        ],
    )?;
    let seconds = number(&options, "--timeout-seconds", 30)?;
    if seconds == 0 || seconds > 600 {
        return Err("timeout-seconds must be 1..600".into());
    }
    let requests = number(&options, "--requests", 64)?;
    let warmups = number(&options, "--warmups", 16)?;
    let repetitions = number(&options, "--repetitions", 3)?;
    if requests > 100000 || warmups > 100000 || repetitions > 100 {
        return Err("requests/warmups capped at100000, repetitions capped at100".into());
    }
    let settings = Settings {
        endpoint: required(&options, "--endpoint")?,
        mode: required(&options, "--mode")?,
        profile: get(&options, "--profile", "smoke"),
        payload_dir: PathBuf::from(required(&options, "--payload-dir")?),
        output: PathBuf::from(required(&options, "--output")?),
        concurrency: list(&options, "--concurrency", "1,8,32")
            .iter()
            .map(|s| {
                s.parse::<usize>()
                    .map_err(|_| "invalid concurrency list".into())
            })
            .collect::<Result<Vec<_>, String>>()?,
        session_models: list(&options, "--session-models", "shared,isolated"),
        channel_models: list(&options, "--channel-models", "shared,isolated"),
        requests,
        warmups,
        repetitions,
        timeout: Duration::from_secs(seconds as u64),
        client_threads: threads,
        case_filter: options.get("--case").cloned(),
        coverage_only: options.contains_key("--coverage-only"),
        max_prepared_bytes: number(&options, "--max-prepared-mib", 256)?
            .checked_mul(1024 * 1024)
            .ok_or("memory budget overflow")?,
    };
    let result = runtime.block_on(load::run(&settings));
    match result {
        Ok(report) => {
            let passed = report["passed"] == true;
            println!(
                "{}",
                json!({"passed":passed,"output":settings.output,"cells":report["cells"].as_array().map(Vec::len)})
            );
            Ok(passed)
        }
        Err(error) => {
            if !settings.output.exists() {
                write(
                    &settings.output,
                    &json!({"schema_version":1,"passed":false,"mode":settings.mode,"error":error}),
                )?;
            }
            Err(error)
        }
    }
}
fn main() {
    match run() {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(error) => {
            eprintln!("protocol benchmark: {error}");
            std::process::exit(2);
        }
    }
}

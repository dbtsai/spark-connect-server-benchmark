use super::*;
use std::sync::atomic::AtomicU64;

struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let path = env::temp_dir().join(format!(
            "protocol-runner-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn options(values: &[(&str, &str)]) -> BTreeMap<String, String> {
    values
        .iter()
        .map(|(k, v)| ((*k).into(), (*v).into()))
        .collect()
}
fn fixture_options(extra: &[(&str, &str)]) -> RunOptions {
    let mut input = options(&[
        ("--output", "unused-test-output"),
        ("--targets", "rust-fixture"),
    ]);
    input.extend(options(extra));
    RunOptions::from_environment(
        &input,
        &BTreeMap::new(),
        &env::current_dir().unwrap(),
        PathBuf::from("/test/bin/protocol"),
    )
    .unwrap()
}
fn parse(input: &[(&str, &str)], environment: &[(&str, &str)]) -> Result<RunOptions, String> {
    let mut all = options(&[("--output", "unused-test-output")]);
    all.extend(options(input));
    let environment = environment
        .iter()
        .map(|(k, v)| (OsString::from(k), OsString::from(v)))
        .collect();
    RunOptions::from_environment(
        &all,
        &environment,
        &env::current_dir().unwrap(),
        PathBuf::from("/test/bin/protocol"),
    )
}

#[test]
fn installations_default_to_environment_and_explicit_paths_override() {
    let env = [
        ("SPARK_HOME", "spark installation"),
        ("JAVA_HOME", "jdk installation"),
    ];
    let parsed = parse(&[], &env).unwrap();
    assert_eq!(
        parsed.spark_home.unwrap(),
        env::current_dir().unwrap().join("spark installation")
    );
    assert_eq!(
        parsed.java_home.unwrap(),
        env::current_dir().unwrap().join("jdk installation")
    );
    let parsed = parse(
        &[
            ("--spark-home", "selected spark"),
            ("--java-home", "selected jdk"),
        ],
        &env,
    )
    .unwrap();
    assert_eq!(
        parsed.spark_home.unwrap(),
        env::current_dir().unwrap().join("selected spark")
    );
    assert_eq!(
        parsed.java_home.unwrap(),
        env::current_dir().unwrap().join("selected jdk")
    );
    assert!(parse(&[("--spark-home", "spark"), ("--java-home", "jdk")], &[]).is_ok());
}
#[test]
fn rust_only_fixture_needs_no_installations_and_does_not_reintroduce_inherited_homes() {
    let parsed = parse(
        &[("--targets", "rust-fixture")],
        &[("SPARK_HOME", ""), ("JAVA_HOME", "")],
    )
    .unwrap();
    assert!(parsed.spark_home.is_none() && parsed.java_home.is_none());
    let result = environment(&parsed, None);
    assert!(!result.contains_key(&OsString::from("SPARK_HOME")));
    assert!(!result.contains_key(&OsString::from("JAVA_HOME")));
    for name in STRIPPED_ENV {
        assert!(!result.contains_key(&OsString::from(*name)));
    }
}
#[test]
fn all_jvm_targets_require_both_nonempty_installations() {
    for target in [
        "jvm-fixture",
        "jvm-planning",
        "rust-jvm-planning",
        "rust-fixture,jvm-fixture",
    ] {
        assert!(parse(&[("--targets", target)], &[])
            .unwrap_err()
            .contains("--spark-home or SPARK_HOME"));
        assert!(parse(
            &[("--targets", target)],
            &[("SPARK_HOME", "spark"), ("JAVA_HOME", "")]
        )
        .unwrap_err()
        .contains("--java-home or JAVA_HOME"));
        assert!(parse(
            &[("--targets", target)],
            &[("SPARK_HOME", ""), ("JAVA_HOME", "jdk")]
        )
        .is_err());
    }
}
#[test]
fn default_artifacts_follow_repository_and_current_executable() {
    let parsed = fixture_options(&[]);
    assert_eq!(parsed.binary, PathBuf::from("/test/bin/protocol"));
    assert_eq!(
        parsed.jar,
        repository_root().join(".build/jvm/spark-connect-native-bridge.jar")
    );
    let suffix = if cfg!(target_os = "macos") {
        ".dylib"
    } else {
        ".so"
    };
    assert_eq!(
        parsed.library,
        repository_root().join(format!("target/release/libspark_connect_jni{suffix}"))
    );
}
#[test]
fn csv_validation_rejects_duplicate_and_invalid_values() {
    assert_eq!(canonical_csv(" 01, 8,32 ", None, true).unwrap(), "1,8,32");
    for value in ["", "1,", "1,,8", "1,1", "1,01", "0", "513", "-1", "a"] {
        assert!(canonical_csv(value, None, true).is_err(), "{value}");
    }
    assert!(canonical_csv("shared,invalid", Some(&["shared", "isolated"]), false).is_err());
    assert!(parse(&[("--targets", "rust-fixture,rust-fixture")], &[]).is_err());
    assert!(parse(&[("--targets", "unknown")], &[]).is_err());
}
#[test]
fn options_reject_unknown_missing_and_out_of_bound_values() {
    let input = options(&[("--targets", "rust-fixture")]);
    assert!(RunOptions::from_environment(
        &input,
        &BTreeMap::new(),
        &env::current_dir().unwrap(),
        "/bin/protocol".into()
    )
    .unwrap_err()
    .contains("--output"));
    for (key, value) in [
        ("--typo", "x"),
        ("--forks", "0"),
        ("--forks", "21"),
        ("--server-threads", "129"),
        ("--client-threads", "0"),
        ("--warmups", "-1"),
        ("--requests", "100001"),
        ("--requests", "31"),
        ("--repetitions", "101"),
        ("--timeout-seconds", "601"),
        ("--startup-timeout", "0"),
        ("--client-timeout", "0"),
        ("--max-prepared-mib", "0"),
        ("--profile", "invalid"),
        ("--coverage-only", "maybe"),
    ] {
        assert!(
            parse(&[("--targets", "rust-fixture"), (key, value)], &[]).is_err(),
            "{key} {value}"
        );
    }
    let parsed = fixture_options(&[
        ("--concurrency", "1"),
        ("--requests", "1"),
        ("--warmups", "0"),
        ("--coverage-only", "true"),
    ]);
    assert_eq!(parsed.warmups, 0);
    assert!(parsed.coverage_only);
}
#[test]
fn path_expansion_and_missing_output_ancestor_are_portable() {
    let temporary = Temporary::new();
    let environment =
        BTreeMap::from([(OsString::from("HOME"), temporary.0.clone().into_os_string())]);
    assert_eq!(
        absolute_path("~/new/output", &temporary.0, &environment).unwrap(),
        fs::canonicalize(&temporary.0).unwrap().join("new/output")
    );
    assert_eq!(
        absolute_path("a/../b", &temporary.0, &environment).unwrap(),
        fs::canonicalize(&temporary.0).unwrap().join("b")
    );
    assert!(absolute_path("~/new", &temporary.0, &BTreeMap::new()).is_err());
}
#[test]
fn environment_retains_fixed_runtime_settings_and_fork_directories() {
    let parsed = fixture_options(&[]);
    let folder = Path::new("/tmp/a fork");
    let result = environment(&parsed, Some(folder));
    assert_eq!(
        result.get(&OsString::from("SPARK_LOCAL_IP")).unwrap(),
        "127.0.0.1"
    );
    assert_eq!(
        result.get(&OsString::from("SPARK_CONNECT_MODE")).unwrap(),
        "0"
    );
    assert_eq!(
        result.get(&OsString::from("SPARK_CONF_DIR")).unwrap(),
        &folder.join("conf").into_os_string()
    );
}
#[test]
fn fixture_server_and_client_commands_preserve_arguments_and_timing_dimensions() {
    let parsed = fixture_options(&[
        ("--profile", "smoke"),
        ("--concurrency", "1,4"),
        ("--requests", "8"),
        ("--warmups", "4"),
        ("--repetitions", "1"),
        ("--case", "analyze/chain"),
        ("--coverage-only", "true"),
    ]);
    let payloads = Path::new("/tmp/corpus with spaces");
    let (command, port, configs) = server_command(
        &parsed,
        "rust-fixture",
        Path::new("/tmp/fork"),
        payloads,
        None,
    )
    .unwrap();
    assert!(port.is_none());
    assert_eq!(configs, json!({}));
    assert_eq!(
        command,
        vec![
            "/test/bin/protocol",
            "serve-fixture",
            "--payload-dir",
            "/tmp/corpus with spaces",
            "--listen",
            "127.0.0.1:0",
            "--threads",
            "4"
        ]
    );
    let command = client_command(
        &parsed,
        "fixture",
        payloads,
        Path::new("/tmp/client.json"),
        1234,
    );
    assert_eq!(
        command[0..5],
        [
            "/test/bin/protocol",
            "run",
            "--endpoint",
            "http://127.0.0.1:1234",
            "--mode"
        ]
    );
    assert!(command
        .windows(2)
        .any(|pair| pair == ["--concurrency", "1,4"]));
    assert!(command.windows(2).any(|pair| pair == ["--requests", "8"]));
    assert!(command
        .windows(2)
        .any(|pair| pair == ["--case", "analyze/chain"]));
    assert_eq!(command.last().unwrap(), "--coverage-only");
}
#[test]
fn planning_commands_retain_real_engine_and_zero_job_listener() {
    let temporary = Temporary::new();
    let mut parsed = fixture_options(&[]);
    parsed.spark_home = Some(temporary.0.clone());
    parsed.java_home = Some(temporary.0.clone());
    for target in ["jvm-planning", "rust-jvm-planning"] {
        let (command, port, configs) = server_command(
            &parsed,
            target,
            Path::new("/tmp/fork"),
            Path::new("/tmp/payloads"),
            Some(1234),
        )
        .unwrap();
        assert_eq!(port, Some(1234));
        assert_eq!(
            configs["spark.extraListeners"],
            "org.apache.spark.sql.connect.nativebridge.benchmark.NoExecutionListener"
        );
        assert_eq!(configs["spark.connect.grpc.binding.port"], "1234");
        assert!(command.contains(&"--driver-memory".into()));
        if target == "jvm-planning" {
            assert!(command.contains(&"--jars".into()));
            assert_eq!(command.last().unwrap(), "spark-internal");
        } else {
            assert!(command.contains(
                &"org.apache.spark.sql.connect.nativebridge.engine.EmbeddedSparkEngine".into()
            ));
            assert_eq!(command.last().unwrap(), "127.0.0.1:1234");
        }
    }
}
#[test]
fn log_parsing_requires_exact_readiness_and_last_audit_marker() {
    assert_eq!(fixture_port("prefix PROTOCOL_FIXTURE_LISTENING 127.0.0.1:12\nPROTOCOL_FIXTURE_LISTENING 127.0.0.1:1234\n"),Some(1234));
    assert_eq!(
        fixture_port("PROTOCOL_FIXTURE_LISTENING 0.0.0.0:12\n"),
        None
    );
    assert_eq!(
        fixture_port("PROTOCOL_FIXTURE_LISTENING 127.0.0.1:0\n"),
        None
    );
    assert_eq!(
        last_json_marker("M {\"a\":1}\nM {\"a\":2}\n", "M").unwrap(),
        Some(json!({"a":2}))
    );
    assert!(last_json_marker("M invalid\n", "M").is_err());
    assert_eq!(parse_lsof("p12\nf7\np5\np12\npinvalid\n"), vec![5, 12]);
}
#[test]
fn linux_listener_parser_matches_only_listening_port_and_inode() {
    let contents="header\n 0: 0100007F:04D2 00000000:0000 0A 0 0 0 0 0 12345\n 1: 0100007F:04D2 00000000:0000 01 0 0 0 0 0 777\n 2: 0100007F:04D3 00000000:0000 0A 0 0 0 0 0 888\nmalformed\n";
    assert_eq!(
        listening_inodes(contents, 1234),
        BTreeSet::from(["12345".into()])
    );
}
#[test]
fn shutdown_requires_successful_exit_zero_executions_and_cleanup_evidence() {
    let fixture = "PROTOCOL_FIXTURE_STATS {\"query_executions\":0}\nPROTOCOL_FIXTURE_SHUTDOWN\n";
    assert_eq!(
        shutdown_evidence(fixture, "rust-fixture", 0).unwrap()["fixture_stats"]["query_executions"],
        0
    );
    assert!(shutdown_evidence(fixture, "jvm-fixture", 1).is_err());
    assert!(shutdown_evidence(
        "PROTOCOL_FIXTURE_STATS {\"query_executions\":1}\nPROTOCOL_FIXTURE_SHUTDOWN\n",
        "rust-fixture",
        0
    )
    .is_err());
    assert!(shutdown_evidence("PROTOCOL_FIXTURE_SHUTDOWN\n", "rust-fixture", 0).is_err());
    assert!(shutdown_evidence(
        &format!("{fixture}WARNING in native method"),
        "rust-fixture",
        0
    )
    .is_err());
    assert!(shutdown_evidence(
        "PROTOCOL_BENCH_SPARK_JOBS {\"jobs_started\":0}\n",
        "jvm-planning",
        -libc::SIGTERM
    )
    .is_ok());
    assert!(shutdown_evidence(
        "PROTOCOL_BENCH_SPARK_JOBS {\"jobs_started\":0}\n",
        "rust-jvm-planning",
        0
    )
    .is_err());
    assert!(shutdown_evidence(
        "PROTOCOL_BENCH_SPARK_JOBS {\"jobs_started\":1}\nEmbedded Spark Engine shutdown complete\n",
        "rust-jvm-planning",
        0
    )
    .is_err());
    assert!(shutdown_evidence(
        "PROTOCOL_BENCH_SPARK_JOBS {\"jobs_started\":0}\nEmbedded Spark Engine shutdown complete\n",
        "rust-jvm-planning",
        0
    )
    .is_ok());
}
#[test]
fn artifact_freeze_preserves_bytes_hashes_and_detects_later_mutation() {
    let temporary = Temporary::new();
    let source = temporary.0.join("source");
    let snapshot = temporary.0.join("snapshot");
    fs::write(&source, b"frozen contents").unwrap();
    let record = copy_artifact(&source, &snapshot).unwrap();
    let artifacts = json!({"binary":record});
    verify_artifacts(&artifacts).unwrap();
    fs::write(&source, b"changed source").unwrap();
    assert!(verify_artifacts(&artifacts)
        .unwrap_err()
        .contains("frozen input changed"));
    fs::write(&source, b"frozen contents").unwrap();
    fs::write(&snapshot, b"changed snapshot").unwrap();
    assert!(verify_artifacts(&artifacts).is_err());
}
fn corpus(temporary: &Temporary) -> (PathBuf, RunOptions) {
    let source = temporary.0.join("original");
    let output = temporary.0.join("run");
    fs::create_dir(&source).unwrap();
    fs::create_dir(&output).unwrap();
    fs::create_dir(source.join("nested")).unwrap();
    fs::write(source.join("nested/schema.pb"), b"schema").unwrap();
    fs::write(source.join("batch.arrow"), b"batch").unwrap();
    fs::write(source.join("plan.pb"), b"plan").unwrap();
    let manifest = json!({"payloads":[{"schema_file":"nested/schema.pb","arrow_file":"batch.arrow"}],"cases":[{"relation_file":"plan.pb","expected_schema_file":"nested/schema.pb"}]});
    fs::write(
        source.join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    let mut parsed = fixture_options(&[]);
    parsed.payload_dir = Some(source.clone());
    parsed.output = output;
    (source, parsed)
}
#[test]
fn corpus_copy_preserves_manifest_and_all_declared_inputs() {
    let temporary = Temporary::new();
    let (source, parsed) = corpus(&temporary);
    let (payloads, hashes) = prepare_payloads(&parsed, &environment(&parsed, None)).unwrap();
    assert_eq!(hashes.len(), 4);
    assert_eq!(
        fs::read(source.join("manifest.json")).unwrap(),
        fs::read(payloads.join("manifest.json")).unwrap()
    );
    assert_eq!(hashes, collect_hashes(&payloads).unwrap());
    fs::write(payloads.join("batch.arrow"), b"changed").unwrap();
    assert_ne!(hashes, collect_hashes(&payloads).unwrap());
}
#[test]
fn corpus_paths_reject_traversal_absolute_and_nonstring_names() {
    for value in ["../escape", "a/../../escape", "/absolute", ""] {
        assert!(safe_relative(value).is_err(), "{value}");
    }
    let temporary = Temporary::new();
    let (source, parsed) = corpus(&temporary);
    fs::write(
        source.join("manifest.json"),
        br#"{"payloads":[{"arrow_file":2}]}"#,
    )
    .unwrap();
    assert!(prepare_payloads(&parsed, &environment(&parsed, None))
        .unwrap_err()
        .contains("string"));
}
#[cfg(unix)]
#[test]
fn corpus_copy_rejects_symlink_escaping_input_root() {
    let temporary = Temporary::new();
    let (source, parsed) = corpus(&temporary);
    fs::remove_file(source.join("batch.arrow")).unwrap();
    fs::write(temporary.0.join("secret"), b"outside corpus").unwrap();
    std::os::unix::fs::symlink(temporary.0.join("secret"), source.join("batch.arrow")).unwrap();
    assert!(prepare_payloads(&parsed, &environment(&parsed, None))
        .unwrap_err()
        .contains("symlink escapes"));
}
#[cfg(unix)]
#[test]
fn command_capture_and_timeout_cleanup_do_not_need_a_socket() {
    let env = environment(&fixture_options(&[]), None);
    let result = capture(
        &[
            "/bin/sh".into(),
            "-c".into(),
            "printf output; printf error >&2; exit 7".into(),
        ],
        &env,
        Duration::from_secs(2),
    )
    .unwrap();
    assert_eq!(result, (7, "output".into(), "error".into()));
    let start = Instant::now();
    assert!(capture(
        &["/bin/sh".into(), "-c".into(), "sleep 30 & wait".into()],
        &env,
        Duration::from_millis(50)
    )
    .unwrap_err()
    .contains("timed out"));
    assert!(start.elapsed() < Duration::from_secs(3));
}
#[cfg(unix)]
#[test]
fn dropping_child_guard_stops_background_descendants() {
    let temporary = Temporary::new();
    let marker = temporary.0.join("child.pid");
    let env = environment(&fixture_options(&[]), None);
    let command = vec![
        "/bin/sh".into(),
        "-c".into(),
        format!("sleep 30 & echo $! > '{}'; wait", marker.display()),
    ];
    let mut child =
        ChildGroup::spawn(&command, &temporary.0, &env, Stdio::null(), Stdio::null()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while !marker.is_file() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    let descendant: i32 = fs::read_to_string(&marker).unwrap().trim().parse().unwrap();
    assert_eq!(
        unsafe { libc::getpgid(descendant) },
        child.child.id() as i32
    );
    child.signal(libc::SIGTERM).unwrap();
    let _ = child.wait(Duration::from_secs(2), false);
    drop(child);
    // A zombie may briefly remain under init, but it cannot retain the process's descriptors.
    let pid_status = capture(
        &[
            "ps".into(),
            "-o".into(),
            "stat=".into(),
            "-p".into(),
            descendant.to_string(),
        ],
        &env,
        Duration::from_secs(2),
    )
    .unwrap();
    assert!(pid_status.1.trim().is_empty() || pid_status.1.trim().starts_with('Z'));
}

#[test]
fn public_run_rejects_mutated_options_before_creating_output() {
    let temporary = Temporary::new();
    for mutation in 0..13 {
        let mut parsed = fixture_options(&[]);
        parsed.output = temporary.0.join(format!("invalid-{mutation}"));
        match mutation {
            0 => parsed.target_list.clear(),
            1 => {
                parsed.targets = "jvm-planning".into();
                parsed.target_list = vec!["jvm-planning".into()];
            }
            2 => parsed.forks = 0,
            3 => parsed.client_threads = 0,
            4 => parsed.server_threads = 129,
            5 => parsed.requests = 0,
            6 => parsed.repetitions = 0,
            7 => parsed.concurrency = "1,01".into(),
            8 => parsed.session_models = "invalid".into(),
            9 => parsed.profile = "invalid".into(),
            10 => parsed.max_prepared_mib = usize::MAX,
            11 => parsed.startup_timeout = 0,
            12 => parsed.binary = PathBuf::from("relative-program"),
            _ => unreachable!(),
        }
        let output = parsed.output.clone();
        assert!(run(parsed).is_err(), "mutation {mutation}");
        assert!(
            !output.exists(),
            "mutation {mutation} created output before validation"
        );
    }
}
#[test]
fn default_repository_root_prefers_runtime_checkout_and_relocated_executable() {
    let temporary = Temporary::new();
    let runtime = temporary.0.join("runtime-checkout");
    fs::create_dir_all(runtime.join("benchmarks/protocol")).unwrap();
    fs::write(runtime.join("Cargo.toml"), b"root marker").unwrap();
    fs::write(
        runtime.join("benchmarks/protocol/Cargo.toml"),
        b"protocol marker",
    )
    .unwrap();
    let executable = runtime.join("target/release/protocol");
    assert_eq!(
        locate_repository_root(Some(&executable), Some(&temporary.0)),
        runtime
    );
    assert_eq!(
        locate_repository_root(
            Some(Path::new("/missing/build-root/target/protocol")),
            Some(&runtime)
        ),
        runtime
    );
}
#[cfg(unix)]
#[test]
fn child_capture_uses_existing_runtime_directory_without_a_source_checkout() {
    let temporary = Temporary::new();
    let root = fs::canonicalize(&temporary.0).unwrap();
    let env = environment(&fixture_options(&[]), None);
    let (code, stdout, stderr) =
        capture_in(&["/bin/pwd".into()], &env, Duration::from_secs(2), &root).unwrap();
    assert_eq!(code, 0);
    assert_eq!(stdout.trim(), text(&root));
    assert!(stderr.is_empty());
    let log = root.join("export.log");
    assert_eq!(
        run_logged(
            &["/bin/pwd".into()],
            &root,
            &env,
            &log,
            Duration::from_secs(2)
        )
        .unwrap(),
        0
    );
    assert_eq!(fs::read_to_string(log).unwrap().trim(), text(&root));
}

#[test]
fn replaying_frozen_harness_and_same_named_artifacts_never_overwrites_identity() {
    let mut names = BTreeSet::from([OsString::from("harness")]);
    assert_eq!(
        artifact_snapshot_name("binary", Path::new("/saved/harness"), &mut names).unwrap(),
        OsString::from("binary-harness")
    );
    assert_eq!(
        artifact_snapshot_name("jar", Path::new("/baseline/artifact"), &mut names).unwrap(),
        OsString::from("artifact")
    );
    assert_eq!(
        artifact_snapshot_name("library", Path::new("/candidate/artifact"), &mut names).unwrap(),
        OsString::from("library-artifact")
    );
    assert_eq!(names.len(), 4);
}

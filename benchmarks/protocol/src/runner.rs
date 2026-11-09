//! Native orchestration for fresh-process protocol and planning benchmark forks.
use crate::harness_util::{harness_identity, now_utc, save, sha_file, SignalLease};
use serde_json::{json, Map, Value};
#[cfg(unix)]
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    ffi::OsString,
    fs::{self, File},
    io::Read,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream},
    path::{Component, Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

const TARGETS: &[&str] = &[
    "rust-fixture",
    "jvm-fixture",
    "jvm-planning",
    "rust-jvm-planning",
];
const OPTIONS: &[&str] = &[
    "--output",
    "--targets",
    "--forks",
    "--binary",
    "--jar",
    "--library",
    "--spark-home",
    "--java-home",
    "--payload-dir",
    "--profile",
    "--concurrency",
    "--session-models",
    "--channel-models",
    "--requests",
    "--warmups",
    "--repetitions",
    "--timeout-seconds",
    "--server-threads",
    "--client-threads",
    "--max-prepared-mib",
    "--case",
    "--coverage-only",
    "--startup-timeout",
    "--client-timeout",
];
const STRIPPED_ENV: &[&str] = &[
    "JAVA_TOOL_OPTIONS",
    "_JAVA_OPTIONS",
    "JDK_JAVA_OPTIONS",
    "SPARK_SUBMIT_OPTS",
    "SPARK_REMOTE",
    "SPARK_API_MODE",
];
static INTERRUPTED: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Debug)]
pub struct RunOptions {
    pub output: PathBuf,
    pub targets: String,
    pub target_list: Vec<String>,
    pub forks: usize,
    pub binary: PathBuf,
    pub jar: PathBuf,
    pub library: PathBuf,
    pub spark_home: Option<PathBuf>,
    pub java_home: Option<PathBuf>,
    pub payload_dir: Option<PathBuf>,
    pub profile: String,
    pub concurrency: String,
    pub session_models: String,
    pub channel_models: String,
    pub requests: usize,
    pub warmups: usize,
    pub repetitions: usize,
    pub timeout_seconds: usize,
    pub server_threads: usize,
    pub client_threads: usize,
    pub max_prepared_mib: usize,
    pub case_filter: Option<String>,
    pub coverage_only: bool,
    pub startup_timeout: usize,
    pub client_timeout: usize,
}
impl RunOptions {
    /// Validate programmatically constructed options before creating any output.
    pub fn validate(&self) -> Result<(), String> {
        let targets = canonical_csv(&self.targets, Some(TARGETS), false)?;
        if targets.split(',').collect::<Vec<_>>()
            != self
                .target_list
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        {
            return Err("targets and target_list disagree".into());
        }
        let requires_jvm = self
            .target_list
            .iter()
            .any(|target| target != "rust-fixture");
        if requires_jvm {
            if self
                .spark_home
                .as_ref()
                .is_none_or(|path| path.as_os_str().is_empty())
            {
                return Err("--spark-home or SPARK_HOME must be set".into());
            }
            if self
                .java_home
                .as_ref()
                .is_none_or(|path| path.as_os_str().is_empty())
            {
                return Err("--java-home or JAVA_HOME must be set".into());
            }
        }
        for (name, path) in [
            ("output", &self.output),
            ("binary", &self.binary),
            ("jar", &self.jar),
            ("library", &self.library),
        ] {
            if path.as_os_str().is_empty() || !path.is_absolute() {
                return Err(format!("{name} must be an absolute path"));
            }
        }
        for (name, path) in [
            ("spark_home", &self.spark_home),
            ("java_home", &self.java_home),
            ("payload_dir", &self.payload_dir),
        ] {
            if path
                .as_ref()
                .is_some_and(|path| path.as_os_str().is_empty() || !path.is_absolute())
            {
                return Err(format!("{name} must be an absolute path"));
            }
        }
        if !["smoke", "standard", "stress"].contains(&self.profile.as_str()) {
            return Err("--profile must be smoke, standard, or stress".into());
        }
        let concurrency = canonical_csv(&self.concurrency, None, true)?;
        if concurrency != self.concurrency {
            return Err("concurrency must be canonical comma-separated integers".into());
        }
        for (name, values) in [
            ("session_models", &self.session_models),
            ("channel_models", &self.channel_models),
        ] {
            if canonical_csv(values, Some(&["shared", "isolated"]), false)? != *values {
                return Err(format!("{name} must be canonical comma-separated models"));
            }
        }
        let max_workers = concurrency
            .split(',')
            .map(|value| value.parse::<usize>().unwrap())
            .max()
            .unwrap();
        if self.requests == 0 || self.requests > 100000 || self.requests < max_workers {
            return Err("requests must cover maximum concurrency and be 1..100000".into());
        }
        if self.warmups > 100000
            || !(1..=100).contains(&self.repetitions)
            || !(1..=600).contains(&self.timeout_seconds)
        {
            return Err("invalid warmups, repetitions, or timeout-seconds".into());
        }
        if !(1..=20).contains(&self.forks)
            || !(1..=128).contains(&self.server_threads)
            || !(1..=128).contains(&self.client_threads)
        {
            return Err("forks must be 1..20 and thread counts must be 1..128".into());
        }
        if self.startup_timeout == 0 || self.client_timeout == 0 || self.max_prepared_mib == 0 {
            return Err("timeouts and max-prepared-mib must be positive".into());
        }
        self.max_prepared_mib
            .checked_mul(1024 * 1024)
            .ok_or("memory budget overflow")?;
        Ok(())
    }
    pub fn from_options(options: &BTreeMap<String, String>) -> Result<Self, String> {
        Self::from_environment(
            options,
            &env::vars_os().collect(),
            &env::current_dir().map_err(err)?,
            env::current_exe().map_err(err)?,
        )
    }
    fn from_environment(
        options: &BTreeMap<String, String>,
        environment: &BTreeMap<OsString, OsString>,
        cwd: &Path,
        executable: PathBuf,
    ) -> Result<Self, String> {
        for key in options.keys() {
            if !OPTIONS.contains(&key.as_str()) {
                return Err(format!("unknown option {key}"));
            }
        }
        let get =
            |key: &str, default: &str| options.get(key).cloned().unwrap_or_else(|| default.into());
        let number = |key: &str, default: usize, allow_zero: bool| -> Result<usize, String> {
            let value = get(key, &default.to_string())
                .parse::<usize>()
                .map_err(|_| format!("{key} must be a nonnegative integer"))?;
            if value == 0 && !allow_zero {
                return Err(format!("{key} must be positive"));
            }
            Ok(value)
        };
        let path = |value: &str| absolute_path(value, cwd, environment);
        let root = repository_root();
        let installation = |key: &str, variable: &str| -> Result<Option<PathBuf>, String> {
            let value = options
                .get(key)
                .filter(|s| !s.is_empty())
                .cloned()
                .or_else(|| {
                    environment
                        .get(&OsString::from(variable))
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string_lossy().into_owned())
                });
            value.map(|v| path(&v)).transpose()
        };
        let targets = canonical_csv(&get("--targets", &TARGETS.join(",")), Some(TARGETS), false)?;
        let target_list: Vec<String> = targets.split(',').map(str::to_owned).collect();
        let spark_home = installation("--spark-home", "SPARK_HOME")?;
        let java_home = installation("--java-home", "JAVA_HOME")?;
        if target_list.iter().any(|target| target != "rust-fixture") {
            if spark_home.is_none() {
                return Err("--spark-home or SPARK_HOME must be set".into());
            }
            if java_home.is_none() {
                return Err("--java-home or JAVA_HOME must be set".into());
            }
        }
        let profile = get("--profile", "standard");
        if !["smoke", "standard", "stress"].contains(&profile.as_str()) {
            return Err("--profile must be smoke, standard, or stress".into());
        }
        let concurrency = canonical_csv(&get("--concurrency", "1,8,32"), None, true)?;
        let session_models = canonical_csv(
            &get("--session-models", "shared,isolated"),
            Some(&["shared", "isolated"]),
            false,
        )?;
        let channel_models = canonical_csv(
            &get("--channel-models", "shared,isolated"),
            Some(&["shared", "isolated"]),
            false,
        )?;
        let requests = number("--requests", 64, false)?;
        let warmups = number("--warmups", 16, true)?;
        let repetitions = number("--repetitions", 3, false)?;
        let forks = number("--forks", 2, false)?;
        let server_threads = number("--server-threads", 4, false)?;
        let client_threads = number("--client-threads", 4, false)?;
        let timeout_seconds = number("--timeout-seconds", 30, false)?;
        let max_prepared_mib = number("--max-prepared-mib", 256, false)?;
        if requests
            < concurrency
                .split(',')
                .map(|v| v.parse::<usize>().unwrap())
                .max()
                .unwrap()
        {
            return Err("requests must cover maximum concurrency".into());
        }
        if forks > 20 || server_threads > 128 || client_threads > 128 {
            return Err("forks <=20 and thread counts <=128 are required".into());
        }
        if requests > 100000 || warmups > 100000 || repetitions > 100 || timeout_seconds > 600 {
            return Err(
                "requests/warmups <=100000, repetitions <=100, timeout-seconds <=600 are required"
                    .into(),
            );
        }
        max_prepared_mib
            .checked_mul(1024 * 1024)
            .ok_or("memory budget overflow")?;
        let output = path(options.get("--output").ok_or("--output is required")?)?;
        let suffix = if cfg!(target_os = "macos") {
            ".dylib"
        } else {
            ".so"
        };
        Ok(Self {
            output,
            targets,
            target_list,
            forks,
            binary: options
                .get("--binary")
                .map(|v| path(v))
                .transpose()?
                .unwrap_or(executable),
            jar: path(&get(
                "--jar",
                &text(&root.join(".build/jvm/spark-connect-native-bridge.jar")),
            ))?,
            library: path(&get(
                "--library",
                &text(&root.join(format!("target/release/libspark_connect_jni{suffix}"))),
            ))?,
            spark_home,
            java_home,
            payload_dir: options.get("--payload-dir").map(|v| path(v)).transpose()?,
            profile,
            concurrency,
            session_models,
            channel_models,
            requests,
            warmups,
            repetitions,
            timeout_seconds,
            server_threads,
            client_threads,
            max_prepared_mib,
            case_filter: options.get("--case").cloned(),
            coverage_only: match options.get("--coverage-only").map(String::as_str) {
                None | Some("false") => false,
                Some("true" | "") => true,
                _ => return Err("--coverage-only is a flag".into()),
            },
            startup_timeout: number("--startup-timeout", 90, false)?,
            client_timeout: number("--client-timeout", 3600, false)?,
        })
    }
}
fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}
fn text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
fn repository_root() -> PathBuf {
    locate_repository_root(
        env::current_exe().ok().as_deref(),
        env::current_dir().ok().as_deref(),
    )
}
fn locate_repository_root(executable: Option<&Path>, cwd: Option<&Path>) -> PathBuf {
    for start in [executable.and_then(Path::parent), cwd]
        .into_iter()
        .flatten()
    {
        for path in start.ancestors() {
            if path.join("benchmarks/protocol/Cargo.toml").is_file()
                && path.join("Cargo.toml").is_file()
            {
                return path.into();
            }
        }
    }
    let compiled = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .unwrap();
    if compiled.is_dir() {
        return compiled.into();
    }
    cwd.filter(|path| path.is_dir())
        .map(Path::to_path_buf)
        .unwrap_or_else(env::temp_dir)
}
fn runtime_cwd() -> Result<PathBuf, String> {
    env::current_dir()
        .ok()
        .filter(|path| path.is_dir())
        .or_else(|| {
            let temporary = env::temp_dir();
            temporary.is_dir().then_some(temporary)
        })
        .ok_or_else(|| "no existing working directory for a child process".into())
}
fn absolute_path(
    value: &str,
    cwd: &Path,
    environment: &BTreeMap<OsString, OsString>,
) -> Result<PathBuf, String> {
    let value = if value == "~" || value.starts_with("~/") {
        let home = environment
            .get(&OsString::from("HOME"))
            .ok_or("cannot expand ~ without HOME")?;
        PathBuf::from(home).join(value.strip_prefix("~/").unwrap_or(""))
    } else {
        PathBuf::from(value)
    };
    let absolute = if value.is_absolute() {
        value
    } else {
        cwd.join(value)
    };
    let mut normalized = PathBuf::new();
    for part in absolute.components() {
        match part {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            _ => normalized.push(part.as_os_str()),
        }
    }
    // Resolve existing ancestors too, without requiring an output directory to exist.
    let mut ancestor = normalized.as_path();
    let mut tail = Vec::new();
    while !ancestor.exists() {
        if let Some(name) = ancestor.file_name() {
            tail.push(name.to_os_string());
        }
        ancestor = ancestor.parent().ok_or("path has no existing ancestor")?;
    }
    let mut result = fs::canonicalize(ancestor).map_err(err)?;
    for part in tail.into_iter().rev() {
        result.push(part);
    }
    Ok(result)
}
fn canonical_csv(value: &str, choices: Option<&[&str]>, integer: bool) -> Result<String, String> {
    let mut seen = BTreeSet::new();
    let mut result = Vec::new();
    for part in value.split(',').map(str::trim) {
        if part.is_empty() {
            return Err("expected distinct comma-separated values".into());
        }
        if let Some(choices) = choices {
            if !choices.contains(&part) {
                return Err(format!("expected values from {}", choices.join(",")));
            }
        }
        let part = if integer {
            let value = part
                .parse::<usize>()
                .map_err(|_| "expected positive integers")?;
            if !(1..=512).contains(&value) {
                return Err("concurrency must be 1..512".into());
            }
            value.to_string()
        } else {
            part.into()
        };
        if !seen.insert(part.clone()) {
            return Err("expected distinct comma-separated values".into());
        }
        result.push(part);
    }
    Ok(result.join(","))
}
fn environment(options: &RunOptions, folder: Option<&Path>) -> BTreeMap<OsString, OsString> {
    let mut result: BTreeMap<OsString, OsString> = env::vars_os().collect();
    for (name, value) in [
        ("SPARK_LOCAL_IP", "127.0.0.1"),
        ("SPARK_CONNECT_MODE", "0"),
        ("RUST_LOG", "warn"),
    ] {
        result.insert(name.into(), value.into());
    }
    for (name, home) in [
        ("JAVA_HOME", &options.java_home),
        ("SPARK_HOME", &options.spark_home),
    ] {
        if let Some(home) = home {
            result.insert(name.into(), home.as_os_str().into());
        } else {
            result.remove(&OsString::from(name));
        }
    }
    for name in STRIPPED_ENV {
        result.remove(&OsString::from(name));
    }
    if let Some(folder) = folder {
        for (name, subdir) in [
            ("SPARK_CONF_DIR", "conf"),
            ("SPARK_LOG_DIR", "logs"),
            ("SPARK_LOCAL_DIRS", "local"),
        ] {
            result.insert(name.into(), folder.join(subdir).into_os_string());
        }
    }
    result
}

#[cfg(unix)]
extern "C" fn interrupt(_signal: libc::c_int) {
    INTERRUPTED.store(true, Ordering::SeqCst);
}
struct Signals {
    _lease: SignalLease,
    #[cfg(unix)]
    previous: Vec<(libc::c_int, libc::sigaction)>,
}
impl Signals {
    fn install() -> Result<Self, String> {
        let lease = SignalLease::acquire()?;
        INTERRUPTED.store(false, Ordering::SeqCst);
        let mut result = Self {
            _lease: lease,
            #[cfg(unix)]
            previous: Vec::new(),
        };
        #[cfg(unix)]
        for signal in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP] {
            let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
            action.sa_sigaction = interrupt as *const () as usize;
            unsafe {
                libc::sigemptyset(&mut action.sa_mask);
            }
            let mut old = unsafe { std::mem::zeroed() };
            if unsafe { libc::sigaction(signal, &action, &mut old) } != 0 {
                return Err(err(std::io::Error::last_os_error()));
            }
            result.previous.push((signal, old));
        }
        Ok(result)
    }
}
impl Drop for Signals {
    fn drop(&mut self) {
        #[cfg(unix)]
        for (signal, action) in self.previous.iter().rev() {
            unsafe {
                libc::sigaction(*signal, action, std::ptr::null_mut());
            }
        }
    }
}
fn check_interrupted() -> Result<(), String> {
    if INTERRUPTED.load(Ordering::SeqCst) {
        Err("benchmark interrupted; child process groups were stopped".into())
    } else {
        Ok(())
    }
}
fn exit_code(status: ExitStatus) -> i32 {
    #[cfg(unix)]
    {
        status
            .code()
            .unwrap_or_else(|| -status.signal().unwrap_or(1))
    }
    #[cfg(not(unix))]
    {
        status.code().unwrap_or(-1)
    }
}
struct ChildGroup {
    child: Child,
}
impl ChildGroup {
    fn spawn(
        command: &[String],
        cwd: &Path,
        env: &BTreeMap<OsString, OsString>,
        out: Stdio,
        stderr: Stdio,
    ) -> Result<Self, String> {
        let (program, args) = command.split_first().ok_or("empty child command")?;
        let mut builder = Command::new(program);
        builder
            .args(args)
            .current_dir(cwd)
            .env_clear()
            .envs(env)
            .stdout(out)
            .stderr(stderr);
        #[cfg(unix)]
        builder.process_group(0);
        Ok(Self {
            child: builder
                .spawn()
                .map_err(|e| format!("cannot start {program}: {e}"))?,
        })
    }
    fn signal(&mut self, signal: i32) -> Result<(), String> {
        #[cfg(unix)]
        unsafe {
            if libc::kill(-(self.child.id() as i32), signal) != 0 {
                return Err(format!(
                    "cannot signal child process group {}: {}",
                    self.child.id(),
                    std::io::Error::last_os_error()
                ));
            }
        }
        #[cfg(not(unix))]
        {
            let _ = signal;
            self.child.kill().map_err(err)?;
        }
        Ok(())
    }
    fn wait(&mut self, timeout: Duration, cancellable: bool) -> Result<ExitStatus, String> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().map_err(err)? {
                return Ok(status);
            }
            if cancellable {
                check_interrupted()?;
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "child {} timed out after {} seconds",
                    self.child.id(),
                    timeout.as_secs()
                ));
            }
            thread::sleep(Duration::from_millis(25));
        }
    }
}
impl Drop for ChildGroup {
    fn drop(&mut self) {
        let _ = self.signal(libc::SIGKILL);
        let _ = self.child.wait();
    }
}
fn run_logged(
    command: &[String],
    cwd: &Path,
    environment: &BTreeMap<OsString, OsString>,
    log: &Path,
    timeout: Duration,
) -> Result<i32, String> {
    check_interrupted()?;
    let stream = File::create(log).map_err(err)?;
    let mut process = ChildGroup::spawn(
        command,
        cwd,
        environment,
        Stdio::from(stream.try_clone().map_err(err)?),
        Stdio::from(stream),
    )?;
    process.wait(timeout, true).map(exit_code)
}
fn capture(
    command: &[String],
    environment: &BTreeMap<OsString, OsString>,
    timeout: Duration,
) -> Result<(i32, String, String), String> {
    capture_in(command, environment, timeout, &runtime_cwd()?)
}
fn capture_in(
    command: &[String],
    environment: &BTreeMap<OsString, OsString>,
    timeout: Duration,
    cwd: &Path,
) -> Result<(i32, String, String), String> {
    check_interrupted()?;
    let mut process = ChildGroup::spawn(command, cwd, environment, Stdio::piped(), Stdio::piped())?;
    let mut stdout = process
        .child
        .stdout
        .take()
        .ok_or("child stdout unavailable")?;
    let mut stderr = process
        .child
        .stderr
        .take()
        .ok_or("child stderr unavailable")?;
    let out = thread::spawn(move || {
        let mut data = String::new();
        stdout.read_to_string(&mut data).map(|_| data)
    });
    let error = thread::spawn(move || {
        let mut data = String::new();
        stderr.read_to_string(&mut data).map(|_| data)
    });
    let result = process.wait(timeout, true);
    // Stop descendants holding pipes before joining readers, including timeout paths.
    let _ = process.signal(libc::SIGKILL);
    let output = out
        .join()
        .map_err(|_| "stdout reader panicked")?
        .map_err(err)?;
    let errors = error
        .join()
        .map_err(|_| "stderr reader panicked")?
        .map_err(err)?;
    Ok((exit_code(result?), output, errors))
}
fn command_output(command: Vec<String>, env: &BTreeMap<OsString, OsString>) -> Value {
    match capture(&command, env, Duration::from_secs(25)) {
        Ok((code, out, error)) => json!({"command":command,"exit_code":code,"output":out+&error}),
        Err(error) => json!({"command":command,"error":error}),
    }
}
fn load_average() -> Value {
    let mut values = [0.0; 3];
    #[cfg(unix)]
    let count = unsafe { libc::getloadavg(values.as_mut_ptr(), 3) };
    #[cfg(not(unix))]
    let count = 0;
    if count == 3 {
        json!(values)
    } else {
        Value::Null
    }
}
fn proc_stats(pid: u32, environment: &BTreeMap<OsString, OsString>) -> Value {
    let command = vec![
        "ps".into(),
        "-p".into(),
        pid.to_string(),
        "-o".into(),
        "pid=,rss=,time=,pcpu=".into(),
    ];
    match capture(&command, environment, Duration::from_secs(10)) {
        Ok((_, output, _)) => {
            json!({"fields":["pid","rss_kib","cpu_time","cpu_percent"],"raw":output.trim()})
        }
        Err(error) => {
            json!({"fields":["pid","rss_kib","cpu_time","cpu_percent"],"raw":"","error":error})
        }
    }
}
fn connected(port: u16) -> bool {
    TcpStream::connect_timeout(
        &SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)),
        Duration::from_millis(200),
    )
    .is_ok()
}
fn free_port() -> Result<u16, String> {
    TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .and_then(|socket| socket.local_addr())
        .map(|a| a.port())
        .map_err(err)
}
fn executable_on_path(name: &str) -> Option<PathBuf> {
    env::var_os("PATH").and_then(|path| {
        env::split_paths(&path)
            .map(|p| p.join(name))
            .find(|p| p.is_file())
    })
}
fn parse_lsof(text: &str) -> Vec<u32> {
    text.lines()
        .filter_map(|line| line.strip_prefix('p').and_then(|s| s.parse().ok()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
fn listener_owners(
    port: u16,
    environment: &BTreeMap<OsString, OsString>,
) -> Result<Vec<u32>, String> {
    let lsof = executable_on_path("lsof").or_else(|| {
        Path::new("/usr/sbin/lsof")
            .is_file()
            .then(|| PathBuf::from("/usr/sbin/lsof"))
    });
    if let Some(lsof) = lsof {
        let command = vec![
            text(&lsof),
            "-nP".into(),
            format!("-iTCP:{port}"),
            "-sTCP:LISTEN".into(),
            "-Fp".into(),
        ];
        let (code, stdout, stderr) = capture(&command, environment, Duration::from_secs(10))?;
        if ![0, 1].contains(&code) {
            return Err(format!(
                "listener ownership inspection failed: {}",
                stderr.trim()
            ));
        }
        return Ok(parse_lsof(&stdout));
    }
    if !Path::new("/proc").is_dir() {
        return Err("listener inspection requires lsof or Linux /proc".into());
    }
    let mut inodes = BTreeSet::new();
    for name in ["/proc/net/tcp", "/proc/net/tcp6"] {
        if let Ok(contents) = fs::read_to_string(name) {
            inodes.extend(listening_inodes(&contents, port));
        }
    }
    let mut owners = BTreeSet::new();
    for process in fs::read_dir("/proc").map_err(err)?.flatten() {
        let Some(pid) = process
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<u32>().ok())
        else {
            continue;
        };
        let Ok(fds) = fs::read_dir(process.path().join("fd")) else {
            continue;
        };
        for fd in fds.flatten() {
            if let Ok(target) = fs::read_link(fd.path()) {
                let target = text(&target);
                if target
                    .strip_prefix("socket:[")
                    .and_then(|s| s.strip_suffix(']'))
                    .is_some_and(|s| inodes.contains(s))
                {
                    owners.insert(pid);
                }
            }
        }
    }
    Ok(owners.into_iter().collect())
}
fn listening_inodes(contents: &str, port: u16) -> BTreeSet<String> {
    contents
        .lines()
        .skip(1)
        .filter_map(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            if fields.len() < 10 || fields[3] != "0A" {
                return None;
            }
            let actual = u16::from_str_radix(fields[1].split(':').next_back()?, 16).ok()?;
            (actual == port).then(|| fields[9].to_owned())
        })
        .collect()
}
fn fixture_port(contents: &str) -> Option<u16> {
    contents.lines().find_map(|line| {
        line.strip_prefix("PROTOCOL_FIXTURE_LISTENING 127.0.0.1:")?
            .parse::<u16>()
            .ok()
            .filter(|port| *port != 0)
    })
}
fn read_log(path: &Path) -> Result<String, String> {
    fs::read(path)
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .map_err(err)
}
fn last_json_marker(contents: &str, marker: &str) -> Result<Option<Value>, String> {
    contents
        .lines()
        .filter_map(|line| line.strip_prefix(marker).and_then(|s| s.strip_prefix(' ')))
        .next_back()
        .map(|line| serde_json::from_str(line).map_err(err))
        .transpose()
}
fn shutdown_evidence(contents: &str, target: &str, code: i32) -> Result<Value, String> {
    if ![0, -libc::SIGTERM, 128 + libc::SIGTERM].contains(&code) {
        return Err(format!("unexpected server exit code {code}"));
    }
    if [
        "WARNING in native method",
        "FATAL ERROR in native method",
        "Java engine handle cleanup failed:",
    ]
    .iter()
    .any(|part| contents.contains(part))
    {
        return Err("JNI cleanup/checking error in server log".into());
    }
    let mut evidence = json!({"exit_code":code,"listener_closed":true});
    if target.ends_with("fixture") {
        if !contents.contains("PROTOCOL_FIXTURE_SHUTDOWN") {
            return Err("fixture shutdown marker missing".into());
        }
        let stats = last_json_marker(contents, "PROTOCOL_FIXTURE_STATS")?
            .ok_or("fixture execution-free audit missing or failed")?;
        if !stats.is_object() || stats.get("query_executions").and_then(Value::as_u64) != Some(0) {
            return Err("fixture execution-free audit missing or failed".into());
        }
        evidence["fixture_stats"] = stats;
    } else {
        let jobs = last_json_marker(contents, "PROTOCOL_BENCH_SPARK_JOBS")?
            .ok_or("planning-only Spark job audit missing or nonzero")?;
        if !jobs.is_object() || jobs.get("jobs_started").and_then(Value::as_u64) != Some(0) {
            return Err("planning-only Spark job audit missing or nonzero".into());
        }
        evidence["spark_job_audit"] = jobs;
        if target == "rust-jvm-planning"
            && !contents.contains("Embedded Spark Engine shutdown complete")
        {
            return Err("native engine shutdown did not complete".into());
        }
    }
    Ok(evidence)
}
fn await_ready(
    process: &mut ChildGroup,
    log: &Path,
    target: &str,
    mut port: Option<u16>,
    timeout: Duration,
    environment: &BTreeMap<OsString, OsString>,
) -> Result<(u16, Vec<u32>), String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        check_interrupted()?;
        if let Some(status) = process.child.try_wait().map_err(err)? {
            return Err(format!(
                "server exited during startup ({}): {}",
                exit_code(status),
                log.display()
            ));
        }
        let content = read_log(log)?;
        if target.ends_with("fixture") {
            port = fixture_port(&content).or(port);
        }
        let identity = format!(
            "Embedded Spark Connect JVM PID {}; Java Connect listener absent",
            process.child.id()
        );
        if target != "rust-jvm-planning" || content.contains(&identity) {
            if let Some(port) = port.filter(|port| connected(*port)) {
                let owners = listener_owners(port, environment)?;
                if owners != [process.child.id()] {
                    return Err(format!(
                        "listener belongs to {owners:?}, expected server PID {}",
                        process.child.id()
                    ));
                }
                return Ok((port, owners));
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(format!("server readiness timeout: {}", log.display()))
}
fn stop(
    process: &mut ChildGroup,
    log: &Path,
    target: &str,
    port: Option<u16>,
) -> Result<Value, String> {
    if let Some(status) = process.child.try_wait().map_err(err)? {
        return Err(format!(
            "server exited before controlled shutdown ({})",
            exit_code(status)
        ));
    }
    process.signal(libc::SIGTERM)?;
    let status = process
        .wait(Duration::from_secs(30), false)
        .map_err(|_| "server did not finish SIGTERM shutdown")?;
    let deadline = Instant::now() + Duration::from_secs(3);
    while port.is_some_and(connected) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(50));
    }
    if port.is_some_and(connected) {
        return Err("server listener stayed open after shutdown".into());
    }
    shutdown_evidence(&read_log(log)?, target, exit_code(status))
}

fn copy_artifact(source: &Path, destination: &Path) -> Result<Value, String> {
    fs::copy(source, destination)
        .map_err(|e| format!("cannot freeze {}: {e}", source.display()))?;
    let digest = sha_file(destination)?;
    if sha_file(source)? != digest {
        return Err(format!(
            "input changed while freezing: {}",
            source.display()
        ));
    }
    Ok(json!({"source":source,"snapshot":destination,"sha256":digest}))
}
fn freeze(options: &mut RunOptions) -> Result<Value, String> {
    let directory = options.output.join("artifacts");
    fs::create_dir(&directory).map_err(err)?;
    let mut records = Map::new();
    let mut names = BTreeSet::from([OsString::from("harness")]);
    let binary = options.binary.clone();
    let mut artifacts = vec![("binary", binary)];
    if options.target_list.iter().any(|t| t != "rust-fixture") {
        artifacts.push(("jar", options.jar.clone()));
    }
    if options.target_list.iter().any(|t| t == "rust-jvm-planning") {
        artifacts.push(("library", options.library.clone()));
    }
    for (name, source) in artifacts {
        let destination = directory.join(artifact_snapshot_name(name, &source, &mut names)?);
        records.insert(name.into(), copy_artifact(&source, &destination)?);
        match name {
            "binary" => options.binary = destination,
            "jar" => options.jar = destination,
            "library" => options.library = destination,
            _ => unreachable!(),
        }
    }
    records.insert(
        "harness".into(),
        copy_artifact(
            &env::current_exe().map_err(err)?,
            &directory.join("harness"),
        )?,
    );
    Ok(Value::Object(records))
}
fn artifact_snapshot_name(
    role: &str,
    source: &Path,
    names: &mut BTreeSet<OsString>,
) -> Result<OsString, String> {
    let mut name = source
        .file_name()
        .ok_or("artifact has no filename")?
        .to_os_string();
    // Replaying artifacts/harness itself must not collide with its saved identity.
    while !names.insert(name.clone()) {
        let mut prefixed = OsString::from(format!("{role}-"));
        prefixed.push(name);
        name = prefixed;
    }
    Ok(name)
}
fn verify_artifacts(artifacts: &Value) -> Result<(), String> {
    for record in artifacts
        .as_object()
        .ok_or("missing frozen artifacts")?
        .values()
    {
        let expected = record["sha256"].as_str().ok_or("missing artifact hash")?;
        for field in ["source", "snapshot"] {
            let path = Path::new(record[field].as_str().ok_or("missing artifact path")?);
            if sha_file(path)? != expected {
                return Err(format!("frozen input changed: {}", path.display()));
            }
        }
    }
    Ok(())
}
fn safe_relative(value: &str) -> Result<PathBuf, String> {
    let path = Path::new(value);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err("payload manifest path escapes its directory".into());
    }
    Ok(path.into())
}
fn collect_hashes(root: &Path) -> Result<BTreeMap<String, String>, String> {
    fn visit(
        root: &Path,
        path: &Path,
        result: &mut BTreeMap<String, String>,
    ) -> Result<(), String> {
        for entry in fs::read_dir(path).map_err(err)? {
            let entry = entry.map_err(err)?;
            let kind = entry.file_type().map_err(err)?;
            if kind.is_symlink() {
                return Err("frozen corpus must not contain symlinks".into());
            }
            if kind.is_dir() {
                visit(root, &entry.path(), result)?;
            } else if kind.is_file() {
                result.insert(
                    text(entry.path().strip_prefix(root).map_err(err)?),
                    sha_file(&entry.path())?,
                );
            } else {
                return Err("frozen corpus contains a non-file entry".into());
            }
        }
        Ok(())
    }
    let mut result = BTreeMap::new();
    visit(root, root, &mut result)?;
    Ok(result)
}
fn prepare_payloads(
    options: &RunOptions,
    environment: &BTreeMap<OsString, OsString>,
) -> Result<(PathBuf, BTreeMap<String, String>), String> {
    let destination = options.output.join("payloads");
    if let Some(source) = &options.payload_dir {
        let bytes = fs::read(source.join("manifest.json")).map_err(err)?;
        let manifest: Value = serde_json::from_slice(&bytes).map_err(err)?;
        let payloads = manifest
            .get("payloads")
            .and_then(Value::as_array)
            .ok_or("payload manifest requires payloads array")?;
        let cases = match manifest.get("cases") {
            None => Vec::new(),
            Some(value) => value
                .as_array()
                .cloned()
                .ok_or("payload manifest cases must be an array")?,
        };
        fs::create_dir(&destination).map_err(err)?;
        for entry in payloads.iter().chain(cases.iter()) {
            if !entry.is_object() {
                return Err("payload manifest entries must be objects".into());
            }
            for field in [
                "schema_file",
                "arrow_file",
                "relation_file",
                "expected_schema_file",
            ] {
                if let Some(value) = entry.get(field) {
                    let name =
                        safe_relative(value.as_str().ok_or("payload filename must be a string")?)?;
                    let original = source.join(&name);
                    let resolved = fs::canonicalize(&original).map_err(err)?;
                    if !resolved.starts_with(fs::canonicalize(source).map_err(err)?) {
                        return Err("payload manifest symlink escapes its directory".into());
                    }
                    let target = destination.join(name);
                    if let Some(parent) = target.parent() {
                        fs::create_dir_all(parent).map_err(err)?;
                    }
                    fs::copy(original, target).map_err(err)?;
                }
            }
        }
        fs::write(destination.join("manifest.json"), bytes).map_err(err)?;
    } else {
        let command = vec![
            text(&options.binary),
            "export".into(),
            "--profile".into(),
            options.profile.clone(),
            "--output".into(),
            text(&destination),
        ];
        if run_logged(
            &command,
            &options.output,
            environment,
            &options.output.join("export.log"),
            Duration::from_secs(120),
        )? != 0
        {
            return Err("corpus export failed; see export.log".into());
        }
    }
    let hashes = collect_hashes(&destination)?;
    Ok((destination, hashes))
}
fn spark_jars(home: &Path) -> Result<PathBuf, String> {
    let assembly = home.join("assembly/target/scala-2.13/jars");
    let result = if assembly.is_dir() {
        assembly
    } else {
        home.join("jars")
    };
    if !result.is_dir() {
        return Err(format!("Spark jar directory missing: {}", result.display()));
    }
    Ok(result)
}
fn server_command(
    options: &RunOptions,
    target: &str,
    folder: &Path,
    payloads: &Path,
    port: Option<u16>,
) -> Result<(Vec<String>, Option<u16>, Value), String> {
    if target == "rust-fixture" {
        return Ok((
            vec![
                text(&options.binary),
                "serve-fixture".into(),
                "--payload-dir".into(),
                text(payloads),
                "--listen".into(),
                "127.0.0.1:0".into(),
                "--threads".into(),
                options.server_threads.to_string(),
            ],
            None,
            json!({}),
        ));
    }
    let java = options.java_home.as_ref().ok_or("JAVA_HOME is required")?;
    let spark = options
        .spark_home
        .as_ref()
        .ok_or("SPARK_HOME is required")?;
    if target == "jvm-fixture" {
        let separator = if cfg!(windows) { ';' } else { ':' };
        return Ok((
            vec![
                text(&java.join("bin/java")),
                "-Xms256m".into(),
                "-Xmx1g".into(),
                "-cp".into(),
                format!(
                    "{}{separator}{}",
                    options.jar.display(),
                    spark_jars(spark)?.join("*").display()
                ),
                "org.apache.spark.sql.connect.nativebridge.benchmark.ProtocolFixtureServer".into(),
                text(payloads),
                "0".into(),
                options.server_threads.to_string(),
            ],
            None,
            json!({}),
        ));
    }
    let port = match port {
        Some(port) => port,
        None => free_port()?,
    };
    let configs = json!({
        "spark.connect.grpc.binding.address":"127.0.0.1", "spark.connect.grpc.binding.port":port.to_string(),
        "spark.connect.grpc.port.maxRetries":"0", "spark.driver.bindAddress":"127.0.0.1", "spark.driver.host":"127.0.0.1",
        "spark.ui.enabled":"false", "spark.sql.shuffle.partitions":"2", "spark.sql.session.timeZone":"UTC",
        "spark.sql.artifact.isolation.enabled":"true", "spark.sql.artifact.isolation.alwaysApplyClassloader":"true",
        "spark.extraListeners":"org.apache.spark.sql.connect.nativebridge.benchmark.NoExecutionListener",
        "spark.sql.warehouse.dir":text(&folder.join("warehouse"))
    });
    let class = if target == "jvm-planning" {
        "org.apache.spark.sql.connect.service.SparkConnectServer"
    } else {
        "org.apache.spark.sql.connect.nativebridge.engine.EmbeddedSparkEngine"
    };
    let mut command = vec![
        text(&spark.join("bin/spark-submit")),
        "--class".into(),
        class.into(),
        "--master".into(),
        "local[2]".into(),
        "--driver-memory".into(),
        "1g".into(),
        "--name".into(),
        folder.file_name().unwrap().to_string_lossy().into_owned(),
    ];
    if target == "jvm-planning" {
        command.extend(["--jars".into(), text(&options.jar)]);
    }
    for (key, value) in configs.as_object().unwrap() {
        command.extend([
            "--conf".into(),
            format!("{key}={}", value.as_str().unwrap()),
        ]);
    }
    if target == "jvm-planning" {
        command.push("spark-internal".into());
    } else {
        command.extend([
            text(&options.jar),
            text(&options.library),
            format!("127.0.0.1:{port}"),
        ]);
    }
    Ok((command, Some(port), configs))
}
fn client_command(
    options: &RunOptions,
    kind: &str,
    payloads: &Path,
    output: &Path,
    port: u16,
) -> Vec<String> {
    let mut command = vec![
        text(&options.binary),
        "run".into(),
        "--endpoint".into(),
        format!("http://127.0.0.1:{port}"),
        "--mode".into(),
        kind.into(),
        "--profile".into(),
        options.profile.clone(),
        "--payload-dir".into(),
        text(payloads),
        "--output".into(),
        text(output),
    ];
    for (key, value) in [
        ("concurrency", options.concurrency.clone()),
        ("session-models", options.session_models.clone()),
        ("channel-models", options.channel_models.clone()),
        ("requests", options.requests.to_string()),
        ("warmups", options.warmups.to_string()),
        ("repetitions", options.repetitions.to_string()),
        ("timeout-seconds", options.timeout_seconds.to_string()),
        ("client-threads", options.client_threads.to_string()),
        ("max-prepared-mib", options.max_prepared_mib.to_string()),
    ] {
        command.extend([format!("--{key}"), value]);
    }
    if let Some(case) = &options.case_filter {
        command.extend(["--case".into(), case.clone()]);
    }
    if options.coverage_only {
        command.push("--coverage-only".into());
    }
    command
}
fn run_fork(
    options: &RunOptions,
    target: &str,
    fork: usize,
    index: usize,
    payloads: &Path,
) -> Result<Value, (Value, String)> {
    let name = format!("fork{fork}-{target}");
    let folder = options.output.join(&name);
    let kind = if target.ends_with("fixture") {
        "fixture"
    } else {
        "planning"
    };
    let mut record = json!({"fork_id":name,"fork":fork,"target":target,"order_index":index,"profile_kind":kind,
        "client_json":format!("{name}/client.json"),"started_at":now_utc(),"load_before":load_average()});
    let mut process = None;
    let mut port = None;
    let log = folder.join("server.log");
    let result = (|| -> Result<(), String> {
        fs::create_dir(&folder).map_err(err)?;
        for part in ["conf", "local", "logs", "warehouse"] {
            fs::create_dir(folder.join(part)).map_err(err)?;
        }
        fs::write(folder.join("conf/log4j2.properties"), "status = error\nname = ProtocolBenchmark\nappender.console.type = Console\nappender.console.name = console\nappender.console.target = SYSTEM_ERR\nappender.console.layout.type = PatternLayout\nappender.console.layout.pattern = %d{HH:mm:ss} %p %c{1}: %m%n\nrootLogger.level = warn\nrootLogger.appenderRef.console.ref = console\n").map_err(err)?;
        let (command, selected, configs) =
            server_command(options, target, &folder, payloads, None)?;
        port = selected;
        record["command"] = json!(command);
        record["spark_configs"] = configs;
        let env = environment(options, Some(&folder));
        let stream = File::create(&log).map_err(err)?;
        process = Some(ChildGroup::spawn(
            &command,
            &folder,
            &env,
            Stdio::from(stream.try_clone().map_err(err)?),
            Stdio::from(stream),
        )?);
        let child = process.as_mut().unwrap();
        record["pid"] = json!(child.child.id());
        let (selected, owners) = await_ready(
            child,
            &log,
            target,
            port,
            Duration::from_secs(options.startup_timeout as u64),
            &env,
        )?;
        port = Some(selected);
        record["port"] = json!(selected);
        record["listener_owner_pids"] = json!(owners);
        record["server_before"] = proc_stats(child.child.id(), &env);
        let client = client_command(
            options,
            kind,
            payloads,
            &folder.join("client.json"),
            selected,
        );
        record["client_command"] = json!(client);
        let code = run_logged(
            &client,
            &folder,
            &env,
            &folder.join("client.log"),
            Duration::from_secs(options.client_timeout as u64),
        )?;
        record["client_exit_code"] = json!(code);
        record["server_after"] = proc_stats(child.child.id(), &env);
        record["load_after"] = load_average();
        if code != 0 {
            return Err(format!(
                "client failed: {}",
                folder.join("client.log").display()
            ));
        }
        let client_result: Value =
            serde_json::from_slice(&fs::read(folder.join("client.json")).map_err(err)?)
                .map_err(err)?;
        if client_result.get("passed") != Some(&Value::Bool(true)) {
            return Err(format!(
                "client validation failed: {}",
                folder.join("client.json").display()
            ));
        }
        Ok(())
    })();
    let mut failure = result.err();
    if let Some(error) = &failure {
        record["error"] = json!(error);
    }
    if let Some(child) = process.as_mut() {
        match stop(child, &log, target, port) {
            Ok(evidence) => {
                record["shutdown"] = evidence;
                record["clean_shutdown"] = json!(true);
            }
            Err(error) => {
                record["clean_shutdown"] = json!(false);
                record["shutdown_error"] = json!(error);
                if failure.is_none() {
                    failure = Some(error);
                }
            }
        }
        // Guard cleanup must finish before recording final exit status.
        if child.child.try_wait().ok().flatten().is_none() {
            let _ = child.signal(libc::SIGKILL);
            let _ = child.child.wait();
        }
        record["server_exit_code"] = child
            .child
            .try_wait()
            .ok()
            .flatten()
            .map(exit_code)
            .map_or(Value::Null, |c| json!(c));
    }
    if let Err(error) = save(&folder.join("fork.json"), &record) {
        failure = Some(format!(
            "{}; cannot save fork: {error}",
            failure.unwrap_or_default()
        ));
    }
    match failure {
        Some(error) => Err((record, error)),
        None => Ok(record),
    }
}

pub fn run(mut options: RunOptions) -> Result<(), String> {
    options.validate()?;
    #[cfg(not(unix))]
    return Err("suite process-group orchestration currently requires macOS or Linux".into());
    let _signals = Signals::install()?;
    if let Some(parent) = options.output.parent() {
        fs::create_dir_all(parent).map_err(err)?;
    }
    fs::create_dir(&options.output).map_err(|e| {
        format!(
            "output directory must be new ({}): {e}",
            options.output.display()
        )
    })?;
    let executable = env::current_exe().map_err(err)?;
    let digest = sha_file(&executable)?;
    let mut manifest = json!({"schema_version":1,"complete":false,"runs":[],"metadata":{
        "started_at":now_utc(),"platform":format!("{}-{}",env::consts::OS,env::consts::ARCH),"machine":env::consts::ARCH,
        "cpu_count":thread::available_parallelism().map(|v| v.get()).ok(),"targets":options.target_list,"forks_per_target":options.forks,
        "profile":options.profile,"coverage_only":options.coverage_only,"concurrency":options.concurrency,"session_models":options.session_models,
        "channel_models":options.channel_models,"requests":options.requests,"warmups":options.warmups,"repetitions":options.repetitions,
        "server_threads":options.server_threads,"client_threads":options.client_threads,"max_prepared_mib":options.max_prepared_mib,"case_filter":options.case_filter,
        "no_query_execution_requested":true,"runner_sha256":digest,"reporter_sha256":digest,
        "harness_kind":"rust","harness_version":1,"orchestration":"rust","harness_identity":harness_identity()
    }});
    save(&options.output.join("manifest.json"), &manifest)?;
    let result = (|| -> Result<(), String> {
        let artifacts = freeze(&mut options)?;
        manifest["metadata"]["artifacts"] = artifacts.clone();
        let env = environment(&options, None);
        if options.target_list.iter().any(|t| t != "rust-fixture") {
            manifest["metadata"]["java_version"] = command_output(
                vec![
                    text(&options.java_home.as_ref().unwrap().join("bin/java")),
                    "-version".into(),
                ],
                &env,
            );
            manifest["metadata"]["spark_version"] = command_output(
                vec![
                    text(
                        &options
                            .spark_home
                            .as_ref()
                            .unwrap()
                            .join("bin/spark-submit"),
                    ),
                    "--version".into(),
                ],
                &env,
            );
        }
        check_interrupted()?;
        let (payloads, hashes) = prepare_payloads(&options, &env)?;
        manifest["metadata"]["payload_sha256"] = json!(hashes);
        manifest["metadata"]["payload_directory"] = json!(payloads);
        save(&options.output.join("manifest.json"), &manifest)?;
        for fork in 1..=options.forks {
            let mut targets = options.target_list.clone();
            if fork % 2 == 0 {
                targets.reverse();
            }
            for (index, target) in targets.iter().enumerate() {
                check_interrupted()?;
                verify_artifacts(&artifacts)?;
                println!("Running fork {fork}: {target}");
                match run_fork(&options, target, fork, index, &payloads) {
                    Ok(record) => manifest["runs"].as_array_mut().unwrap().push(record),
                    Err((record, error)) => {
                        manifest["runs"].as_array_mut().unwrap().push(record);
                        return Err(error);
                    }
                }
                save(&options.output.join("manifest.json"), &manifest)?;
            }
        }
        verify_artifacts(&artifacts)?;
        if collect_hashes(&payloads)? != hashes {
            return Err("frozen payload files changed during the run".into());
        }
        manifest["complete"] = json!(true);
        Ok(())
    })();
    if let Err(error) = &result {
        manifest["error"] = json!(error);
    }
    save(&options.output.join("manifest.json"), &manifest)?;
    result
}

#[cfg(test)]
#[path = "runner_tests.rs"]
mod tests;

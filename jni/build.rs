fn main() {
    let java_home = std::env::var_os("JAVA_HOME")
        .expect("Set JAVA_HOME to a JDK with JNI headers before building spark-connect-jni");
    let include = std::path::PathBuf::from(java_home).join("include");
    let platform = match std::env::var("CARGO_CFG_TARGET_OS").unwrap().as_str() {
        "macos" => "darwin",
        "linux" => "linux",
        "windows" => "win32",
        other => panic!("unsupported JNI target: {other}"),
    };
    cc::Build::new()
        .file("native/bridge.c")
        .include(&include)
        .include(include.join(platform))
        .std("c11")
        .warnings(true)
        .compile("ferrum_jni_bridge");
    println!("cargo:rerun-if-env-changed=JAVA_HOME");
    println!("cargo:rerun-if-changed=native/bridge.c");
    println!("cargo:rerun-if-changed=native/bridge.h");
}

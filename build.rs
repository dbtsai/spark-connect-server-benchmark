use tonic_build::manual::{Builder, Method, Service};

fn main() {
    // The official protocol crate already generates messages and client bindings.
    // Generate only the standard service routes, using those exact message types.
    let methods = [
        (
            "execute_plan",
            "ExecutePlan",
            "ExecutePlanRequest",
            "ExecutePlanResponse",
            false,
            true,
        ),
        (
            "analyze_plan",
            "AnalyzePlan",
            "AnalyzePlanRequest",
            "AnalyzePlanResponse",
            false,
            false,
        ),
        (
            "config",
            "Config",
            "ConfigRequest",
            "ConfigResponse",
            false,
            false,
        ),
        (
            "add_artifacts",
            "AddArtifacts",
            "AddArtifactsRequest",
            "AddArtifactsResponse",
            true,
            false,
        ),
        (
            "artifact_status",
            "ArtifactStatus",
            "ArtifactStatusesRequest",
            "ArtifactStatusesResponse",
            false,
            false,
        ),
        (
            "interrupt",
            "Interrupt",
            "InterruptRequest",
            "InterruptResponse",
            false,
            false,
        ),
        (
            "reattach_execute",
            "ReattachExecute",
            "ReattachExecuteRequest",
            "ExecutePlanResponse",
            false,
            true,
        ),
        (
            "release_execute",
            "ReleaseExecute",
            "ReleaseExecuteRequest",
            "ReleaseExecuteResponse",
            false,
            false,
        ),
        (
            "release_session",
            "ReleaseSession",
            "ReleaseSessionRequest",
            "ReleaseSessionResponse",
            false,
            false,
        ),
        (
            "fetch_error_details",
            "FetchErrorDetails",
            "FetchErrorDetailsRequest",
            "FetchErrorDetailsResponse",
            false,
            false,
        ),
        (
            "clone_session",
            "CloneSession",
            "CloneSessionRequest",
            "CloneSessionResponse",
            false,
            false,
        ),
        (
            "get_status",
            "GetStatus",
            "GetStatusRequest",
            "GetStatusResponse",
            false,
            false,
        ),
    ];
    let mut service = Service::builder()
        .name("SparkConnectService")
        .package("spark.connect");
    for (name, route, input, output, client_stream, server_stream) in methods {
        let mut method = Method::builder()
            .name(name)
            .route_name(route)
            .input_type(format!("spark_connect_proto::{input}"))
            .output_type(format!("spark_connect_proto::{output}"))
            .codec_path("tonic_prost::ProstCodec");
        if client_stream {
            method = method.client_streaming();
        }
        if server_stream {
            method = method.server_streaming();
        }
        service = service.method(method.build());
    }
    Builder::new()
        .build_client(false)
        .compile(&[service.build()]);
    println!("cargo:rerun-if-changed=build.rs");
}

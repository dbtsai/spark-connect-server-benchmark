pub mod api_audit;
pub mod compare;
pub mod corpus;
pub mod fixture;
pub mod harness_util;
pub mod load;
pub mod probes;
pub mod report;
pub mod runner;

#[allow(clippy::double_must_use)]
pub mod grpc {
    include!(concat!(
        env!("OUT_DIR"),
        "/spark.connect.SparkConnectService.rs"
    ));
}

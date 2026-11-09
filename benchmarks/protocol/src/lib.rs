pub mod corpus;
pub mod fixture;
pub mod load;
pub mod probes;

#[allow(clippy::double_must_use)]
pub mod grpc {
    include!(concat!(
        env!("OUT_DIR"),
        "/spark.connect.SparkConnectService.rs"
    ));
}

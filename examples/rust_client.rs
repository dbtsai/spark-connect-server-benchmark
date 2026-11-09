//! Run a server first, then `cargo run --example rust_client`.

use spark_connect::functions as f;
use spark_connect::{col, SparkSession};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let remote = std::env::var("SPARK_REMOTE").unwrap_or_else(|_| "sc://127.0.0.1:15002".into());
    let spark = SparkSession::builder().remote(&remote).get_or_create()?;

    let totals = spark
        .range(1_000)?
        .agg(vec![f::sum(col("id")).alias("total").expression().clone()]);
    println!("Range aggregate: {:?}", totals.collect()?);
    println!("SQL: {:?}", spark.sql("SELECT 1 + 1 AS answer")?.collect()?);

    spark.stop()?;
    Ok(())
}

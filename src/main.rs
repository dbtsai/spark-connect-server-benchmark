use std::sync::Arc;

use spark_connect_server::engine::DataFusionEngine;
use spark_connect_server::jvm::SparkJvm;
use spark_connect_server::Backend;

const USAGE: &str = "Usage: spark-connect-server [LISTEN_ADDRESS] [--backend datafusion|jvm] [--upstream http://HOST:PORT]\n\nDefaults: 127.0.0.1:15002, datafusion. JVM mode requires --upstream.";

struct Options {
    address: String,
    backend: String,
    upstream: Option<String>,
}

impl Options {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Option<Self>, String> {
        let mut args = args.into_iter();
        let mut address = None;
        let mut backend = None;
        let mut upstream = None;
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-h" | "--help" => return Ok(None),
                "--backend" | "--upstream" => {
                    let target = if arg == "--backend" {
                        &mut backend
                    } else {
                        &mut upstream
                    };
                    if target.is_some() {
                        return Err(format!("{arg} may only be specified once"));
                    }
                    *target = Some(
                        args.next()
                            .ok_or_else(|| format!("missing value for {arg}"))?,
                    );
                }
                value if value.starts_with('-') => return Err(format!("unknown option {value}")),
                _ => {
                    if address.replace(arg).is_some() {
                        return Err("only one listen address is allowed".into());
                    }
                }
            }
        }
        let backend = backend.unwrap_or_else(|| "datafusion".into());
        match (backend.as_str(), upstream.is_some()) {
            ("jvm", false) => return Err("--backend jvm requires --upstream".into()),
            ("datafusion", true) => return Err("--upstream requires --backend jvm".into()),
            ("datafusion" | "jvm", _) => {}
            _ => return Err(format!("unknown backend {backend}")),
        }
        Ok(Some(Self {
            address: address.unwrap_or_else(|| "127.0.0.1:15002".into()),
            backend,
            upstream,
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Some(options) = Options::parse(std::env::args().skip(1))? else {
        println!("{USAGE}");
        return Ok(());
    };
    let backend = match options.upstream {
        Some(upstream) => Backend::SparkJvm(SparkJvm::connect(upstream).await?),
        None => Backend::Native(Arc::new(DataFusionEngine)),
    };
    let listener = tokio::net::TcpListener::bind(&options.address).await?;
    println!(
        "{} Spark Connect prototype listening on {}",
        options.backend,
        listener.local_addr()?
    );
    spark_connect_server::serve_with_backend(listener, backend, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Options;

    fn parse(args: &[&str]) -> Result<Option<Options>, String> {
        Options::parse(args.iter().map(|arg| arg.to_string()))
    }

    #[test]
    fn existing_listen_argument_and_default_backend_are_preserved() {
        let options = parse(&["127.0.0.1:15004"]).unwrap().unwrap();
        assert_eq!(options.address, "127.0.0.1:15004");
        assert_eq!(options.backend, "datafusion");
        assert!(options.upstream.is_none());
    }

    #[test]
    fn jvm_selection_requires_an_explicit_upstream() {
        assert!(parse(&["--backend", "jvm"]).is_err());
        assert!(parse(&["--upstream", "http://127.0.0.1:15003"]).is_err());
        assert!(parse(&["--backend", "other"]).is_err());
        let options = parse(&[
            "--backend",
            "jvm",
            "127.0.0.1:15004",
            "--upstream",
            "http://127.0.0.1:15003",
        ])
        .unwrap()
        .unwrap();
        assert_eq!(options.backend, "jvm");
        assert_eq!(options.address, "127.0.0.1:15004");
        assert_eq!(options.upstream.as_deref(), Some("http://127.0.0.1:15003"));
    }
}

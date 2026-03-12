mod config;
mod quic;
mod quinn_extensions;
mod udp;
mod util;

use crate::config::NetworkConfig;
use crate::config::cli::{Command, NetworkOpt};
use crate::config::network::NetworkEventsJson;
use crate::udp::{ping, throughput};
use anyhow::Context;
use clap::Parser;
use config::cli::CliOpt;
use in_memory_network::async_rt;
use in_memory_network::async_rt::DelayMode;
use serde::de::DeserializeOwned;
use std::fs::File;
use std::path::Path;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::Subscriber;

fn main() -> anyhow::Result<()> {
    Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_ansi(false)
        .without_time()
        .init();

    // Safety: we are fully single-threaded
    unsafe { std::env::set_var("SSLKEYLOGFILE", "keylog.key") };
    let opt = CliOpt::parse();

    let delay_mode = if opt.disable_time_warping {
        DelayMode::Wait
    } else {
        DelayMode::TimeWarp
    };
    let rt = async_rt::new_rt(delay_mode);
    match &opt.command {
        Command::Quic(quic_opt) => rt.block_on(quic::run_and_report_stats(quic_opt, delay_mode)),
        Command::Ping(ping_opt) => {
            let network_config = load_network_config(&ping_opt.network)?;
            rt.block_on(ping::run(ping_opt, network_config, delay_mode))
        }
        Command::Throughput(throughput_opt) => {
            let network_config = load_network_config(&throughput_opt.network)?;
            rt.block_on(throughput::run(throughput_opt, network_config, delay_mode))
        }
        Command::Rt => {
            println!("tokio");
            Ok(())
        }
    }
}

fn load_network_config(cli: &NetworkOpt) -> anyhow::Result<NetworkConfig> {
    let network_graph = load_json(&cli.network_graph)?;
    let network_events: NetworkEventsJson = load_json(&cli.network_events)?;

    Ok(NetworkConfig {
        network_graph,
        network_events: network_events.events,
    })
}

fn load_json<T: DeserializeOwned>(path: &Path) -> anyhow::Result<T> {
    let file =
        File::open(path).with_context(|| format!("unable to open file at `{}`", path.display()))?;
    let parsed = serde_json::from_reader(file)
        .with_context(|| format!("error parsing JSON from `{}`", path.display()))?;
    Ok(parsed)
}

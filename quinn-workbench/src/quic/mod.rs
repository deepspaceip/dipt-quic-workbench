use crate::config::cli::QuicOpt;
use crate::config::quinn::{CongestionControlAlgorithm, QuinnJsonConfig};
use crate::load_network_config;
use crate::quic::simulation::QuicSimulation;
use crate::quinn_extensions::ecn_cc::EcnCcFactory;
use crate::quinn_extensions::no_cc::NoCCConfig;
use crate::util::{print_link_stats, print_max_buffer_usage_per_node, print_node_stats};
use anyhow::Context;
use quinn_proto::congestion::{CubicConfig, NewRenoConfig};
use quinn_proto::{AckFrequencyConfig, EndpointConfig, TransportConfig, VarInt};
use std::fs;
use std::sync::Arc;
use std::time::Duration;

mod client;
mod server;
pub mod simulation;

pub async fn run_and_report_stats(quic_options: &QuicOpt) -> anyhow::Result<()> {
    let mut simulation = QuicSimulation::new();
    let network_config = load_network_config(&quic_options.network)?;
    let result = simulation.run(quic_options, network_config).await;

    let Some((tracer, network)) = simulation.tracer_and_network else {
        eprintln!("Error...");
        return result;
    };

    println!("--- Replay log ---");
    let replay_log_path = "replay-log.json";
    let json_steps = serde_json::to_vec_pretty(&tracer.stepper().steps()).unwrap();
    fs::write(replay_log_path, json_steps).context("failed to store replay log")?;
    println!("* Replay log available at {replay_log_path}");

    println!("--- Node stats ---");
    let verified_simulation = tracer
        .verifier()
        .context("failed to create simulation verifier")?
        .verify()
        .context("failed to verify simulation")?;
    let server_node = network.host(quic_options.network.server_ip_address);
    let client_node = network.host(quic_options.network.client_ip_address);
    print_node_stats(&verified_simulation, server_node, client_node);
    print_max_buffer_usage_per_node(&verified_simulation);
    print_link_stats(&verified_simulation, &network);

    const DISPLAY_MAX_ERRORS: usize = 10;
    if !verified_simulation.non_fatal_errors.is_empty() {
        print!("--- Errors");
        if verified_simulation.non_fatal_errors.len() > DISPLAY_MAX_ERRORS {
            print!(
                "(showing {DISPLAY_MAX_ERRORS} of {})",
                verified_simulation.non_fatal_errors.len()
            );
        }

        println!(" ---");
    }
    for error in verified_simulation
        .non_fatal_errors
        .into_iter()
        .take(DISPLAY_MAX_ERRORS)
    {
        println!("* {error}");
    }

    if result.is_err() {
        eprintln!("Error...");
    }

    result
}

fn endpoint_config(rng_seed: [u8; 32]) -> EndpointConfig {
    let mut config = EndpointConfig::default();
    config.rng_seed(Some(rng_seed));

    config
}

fn transport_config(quinn_config: &QuinnJsonConfig) -> TransportConfig {
    let mut config = TransportConfig::default();

    if !quinn_config.mtu_discovery {
        config.mtu_discovery_config(None);
    }

    config.max_idle_timeout(Some(
        Duration::from_millis(quinn_config.maximum_idle_timeout_ms)
            .try_into()
            .unwrap(),
    ));

    if quinn_config.maximize_send_and_receive_windows {
        config.receive_window(VarInt::MAX);
        config.stream_receive_window(VarInt::MAX);
        config.send_window(u64::MAX);
    }

    config.packet_threshold(quinn_config.packet_threshold);

    let get_congestion_window_bytes = |packets: u64| packets * BASE_DATAGRAM_SIZE;
    let cc_factory: Arc<dyn quinn_proto::congestion::ControllerFactory + Send + Sync> =
        match quinn_config.congestion_controller {
            CongestionControlAlgorithm::Cubic => {
                let mut cfg = CubicConfig::default();
                if let Some(packets) = quinn_config.initial_congestion_window_packets {
                    cfg.initial_window(get_congestion_window_bytes(packets));
                }
                Arc::new(cfg)
            }
            CongestionControlAlgorithm::NewReno => {
                let mut cfg = NewRenoConfig::default();
                if let Some(packets) = quinn_config.initial_congestion_window_packets {
                    cfg.initial_window(get_congestion_window_bytes(packets));
                }
                Arc::new(cfg)
            }
            CongestionControlAlgorithm::NoCc => {
                let congestion_window_bytes =
                    if let Some(packets) = quinn_config.initial_congestion_window_packets {
                        get_congestion_window_bytes(packets)
                    } else {
                        u64::MAX
                    };
                Arc::new(NoCCConfig {
                    initial_window: congestion_window_bytes,
                })
            }
            CongestionControlAlgorithm::EcnReno => {
                let mut cfg = NewRenoConfig::default();
                if let Some(packets) = quinn_config.initial_congestion_window_packets {
                    cfg.initial_window(get_congestion_window_bytes(packets));
                }
                Arc::new(EcnCcFactory::new(cfg))
            }
        };
    config.congestion_controller_factory(cc_factory);

    let mut ack_frequency_config = AckFrequencyConfig::default();
    ack_frequency_config
        .ack_eliciting_threshold(VarInt::from_u32(quinn_config.ack_eliciting_threshold));
    ack_frequency_config.max_ack_delay(Some(Duration::from_millis(quinn_config.max_ack_delay_ms)));

    // The docs say the recommended value for this is `packet_threshold - 1`
    ack_frequency_config.reordering_threshold(VarInt::from_u32(quinn_config.packet_threshold - 1));
    config.ack_frequency_config(Some(ack_frequency_config));

    config.initial_rtt(Duration::from_millis(quinn_config.initial_rtt_ms));

    config
}

const BASE_DATAGRAM_SIZE: u64 = 1200;

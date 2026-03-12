use crate::config::NetworkConfig;
use crate::config::cli::PingOpt;
use anyhow::Context as _;
use fastrand::Rng;
use in_memory_network::async_rt;
use in_memory_network::async_rt::DelayMode;
use in_memory_network::async_rt::time::Instant;
use in_memory_network::network::InMemoryNetwork;
use in_memory_network::network::event::NetworkEvents;
use in_memory_network::network::spec::NetworkSpec;
use in_memory_network::pcap_exporter::FileBasedPcapExporterFactory;
use in_memory_network::quinn_interop::BufsAndMeta;
use in_memory_network::tracing::tracer::SimulationStepTracer;
use parking_lot::Mutex;
use quinn::AsyncUdpSocket;
use quinn::udp::Transmit;
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

pub async fn run(
    ping_opt: &PingOpt,
    network_config: NetworkConfig,
    delay_mode: DelayMode,
) -> anyhow::Result<()> {
    let simulation_start = Instant::now();

    // Network
    let network_spec: NetworkSpec = network_config.network_graph.into();
    let network_events = NetworkEvents::new(
        network_config
            .network_events
            .clone()
            .into_iter()
            .map(|e| e.into())
            .collect(),
        &network_spec.links,
    );
    let tracer = Arc::new(SimulationStepTracer::new(network_spec.clone()));
    let network = InMemoryNetwork::initialize(
        network_spec.clone(),
        network_events,
        tracer.clone(),
        Arc::new(FileBasedPcapExporterFactory),
        Rng::with_seed(ping_opt.network.network_rng_seed),
        simulation_start,
        delay_mode,
    )?;

    println!("--- Network ---");
    println!("* Initial link statuses (derived from events):");
    for link_spec in &network_spec.links {
        let status = network.get_link_status(&link_spec.id);
        println!("  * {}: {}", link_spec.id, status);
    }

    println!("--- Ping ---");
    let duration = Duration::from_millis(ping_opt.duration_ms);
    let deadline = Duration::from_millis(ping_opt.deadline_ms);
    let interval = Duration::from_millis(ping_opt.interval_ms);

    let server_ip = ping_opt.network.server_ip_address;
    let server_node = network.host(server_ip);
    let server_socket = Arc::pin(network.udp_socket_for_node(server_node.clone()));

    let client_ip = ping_opt.network.client_ip_address;
    let client_node = network.host(client_ip);
    let client_socket = Arc::pin(network.udp_socket_for_node(client_node.clone()));

    // Server
    let server_socket_cp = server_socket.clone();
    async_rt::spawn(async move {
        let mut bufs_and_meta = BufsAndMeta::new(1200, 5);

        loop {
            // Receive next transmits
            for packet in server_socket.receive(&mut bufs_and_meta).await.unwrap() {
                assert_eq!(packet.source_addr.ip(), client_ip);

                // Echo transmit
                server_socket_cp
                    .try_send(&Transmit {
                        destination: SocketAddr::new(client_ip, 8080),
                        ecn: None,
                        contents: packet.payload,
                        segment_size: None,
                        src_ip: None,
                    })
                    .unwrap();
            }
        }
    });

    // -- Client --
    let in_flight = Arc::new(Mutex::new(HashMap::new()));
    let lost = Arc::new(Mutex::new(Vec::new()));

    // Sender
    let client_socket_cp = client_socket.clone();
    let in_flight_cp = in_flight.clone();
    let lost_cp = lost.clone();
    async_rt::spawn(async move {
        let mut ping_nr: u64 = 0;
        loop {
            // Send ping
            let payload = ping_nr.to_le_bytes();

            client_socket_cp
                .try_send(&Transmit {
                    destination: SocketAddr::new(server_ip, 8080),
                    ecn: None,
                    contents: &payload,
                    segment_size: None,
                    src_ip: None,
                })
                .unwrap();

            in_flight_cp.lock().insert(ping_nr, Instant::now());
            ping_nr += 1;

            // Track pings as lost after the deadline has passed
            let in_flight_cp = in_flight_cp.clone();
            let lost_cp = lost_cp.clone();
            async_rt::spawn(async move {
                async_rt::time::sleep(deadline).await;
                if let Some(ping_sent) = in_flight_cp.lock().remove(&ping_nr) {
                    lost_cp.lock().push(ping_nr);
                    let ping_lost = Instant::now();
                    println!(
                        "{:.2}s - SENT | {:.2}s - LOST | {:.2}s - DURATION",
                        (ping_sent - simulation_start).as_secs_f64(),
                        (ping_lost - simulation_start).as_secs_f64(),
                        (ping_lost - ping_sent).as_secs_f64()
                    );
                }
            });

            // Sleep before sending the next ping
            async_rt::time::sleep(interval).await;
        }
    });

    // Receiver
    async_rt::spawn(async move {
        let mut bufs_and_meta = BufsAndMeta::new(1200, 5);

        loop {
            // Receive next transmits
            for packet in client_socket.receive(&mut bufs_and_meta).await.unwrap() {
                assert_eq!(packet.source_addr.ip(), server_ip);

                let ping_nr = u64::from_le_bytes(packet.payload.try_into().unwrap());

                if let Some(ping_sent) = in_flight.lock().remove(&ping_nr) {
                    let ping_received = Instant::now();
                    println!(
                        "{:.2}s - SENT | {:.2}s - RECEIVED | {:.2}s - DURATION",
                        (ping_sent - simulation_start).as_secs_f64(),
                        (ping_received - simulation_start).as_secs_f64(),
                        (ping_received - ping_sent).as_secs_f64()
                    );
                }
            }
        }
    });

    // Wait till done
    async_rt::time::sleep(duration).await;
    println!("{:.2}s Done", simulation_start.elapsed().as_secs_f64());

    println!("--- Replay log ---");
    let replay_log_path = "replay-log.json";
    let json_steps = serde_json::to_vec_pretty(&tracer.stepper().steps()).unwrap();
    fs::write(replay_log_path, json_steps).context("failed to store replay log")?;
    println!("* Replay log available at {replay_log_path}");

    Ok(())
}

use crate::config::NetworkConfig;
use crate::config::cli::QuicOpt;
use crate::quic::{client, server};
use anyhow::{Context, bail};
use async_lock::Semaphore;
use fastrand::Rng;
use futures::StreamExt;
use in_memory_network::async_rt;
use in_memory_network::async_rt::time::Instant;
use in_memory_network::network::InMemoryNetwork;
use in_memory_network::network::event::NetworkEvents;
use in_memory_network::network::spec::NetworkSpec;
use in_memory_network::pcap_exporter::{FileBasedPcapExporterFactory, NoOpPcapExporterFactory};
use in_memory_network::tracing::tracer::SimulationStepTracer;
use parking_lot::Mutex;
use quinn_proto::VarInt;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
pub struct QuicSimulation {
    pub tracer_and_network: Option<(Arc<SimulationStepTracer>, Arc<InMemoryNetwork>)>,
}

impl QuicSimulation {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn run(
        &mut self,
        quic_options: &QuicOpt,
        network_config: NetworkConfig,
    ) -> anyhow::Result<()> {
        println!("--- Params ---");
        let (quinn_rng_seed, simulated_network_rng_seed) = if quic_options.network.non_deterministic
        {
            let mut rng = Rng::new();
            (rng.u64(..), rng.u64(..))
        } else {
            (
                quic_options.network.quinn_rng_seed,
                quic_options.network.network_rng_seed,
            )
        };
        println!("* Quinn seed: {}", quinn_rng_seed);
        println!("* Network seed: {}", simulated_network_rng_seed);
        println!(
            "* Network graph path: {}",
            quic_options.network.network_graph.display()
        );
        println!(
            "* Network events path: {}",
            quic_options.network.network_events.display()
        );

        let start = Instant::now();

        let quic_configs = network_config.network_graph.quic_configs();

        // Network check
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
        let network = InMemoryNetwork::initialize(
            network_spec.clone(),
            network_events.clone(),
            Arc::new(SimulationStepTracer::new(network_spec.clone())),
            Arc::new(NoOpPcapExporterFactory),
            Rng::with_seed(simulated_network_rng_seed),
            start,
        )?;

        println!("--- Network ---");
        println!("* Initial link statuses (derived from events):");
        for link_spec in &network_spec.links {
            let status = network.get_link_status(&link_spec.id);
            println!("  * {}: {}", link_spec.id, status);
        }
        println!("* Running connectivity check...");
        let server_node = network.host(quic_options.network.server_ip_address);
        let client_node = network.host(quic_options.network.client_ip_address);
        let (arrived1, arrived2) = network
            .assert_connectivity_between_hosts(server_node, client_node)
            .await?;
        println!(
            "* Connectivity check passed (packets arrived after {} ms and {} ms)",
            arrived1.as_millis(),
            arrived2.as_millis()
        );
        drop(network);

        let start = Instant::now();

        // Network
        let tracer = Arc::new(SimulationStepTracer::new(network_spec.clone()));
        let network = InMemoryNetwork::initialize(
            network_spec,
            network_events,
            tracer.clone(),
            Arc::new(FileBasedPcapExporterFactory),
            Rng::with_seed(simulated_network_rng_seed),
            start,
        )?;
        self.tracer_and_network = Some((tracer.clone(), network.clone()));

        // Set up server certificate
        let server_name = "server-name";
        let key = PrivatePkcs8KeyDer::from(server::KEY_PAIR_DER_RSA);
        let cert = CertificateDer::from(server::CERT_DER_RSA);

        // Let a server listen in the background
        let mut quinn_rng = Rng::with_seed(quinn_rng_seed);
        let server_host = network.host(quic_options.network.server_ip_address);
        let server_addr = server_host.quic_addr();
        let server = server::server_endpoint(
            cert.clone(),
            key.into(),
            network.udp_socket_for_node(server_host.clone()),
            &quic_configs[server_host.id().as_ref()],
            &mut quinn_rng,
            server_host.id(),
        )?;
        let mut server_handled_connections =
            server::server_listen(server.clone(), quic_options.response_size);

        // Create the client endpoint
        let client_host = network.host(quic_options.network.client_ip_address);
        let client = client::client_endpoint(
            cert,
            network.udp_socket_for_node(client_host.clone()),
            &quic_configs[client_host.id().as_ref()],
            &mut quinn_rng,
            client_host.id(),
        )?;

        let max_connections = b'Z' - b'A';
        if quic_options.concurrent_connections > max_connections {
            bail!(
                "The maximum number of concurrent connections is {max_connections}, but {} were configured",
                quic_options.concurrent_connections
            );
        }

        // Make requests, potentially using concurrent connections
        println!("--- Requests ---");
        let connections_semaphore =
            Arc::new(Semaphore::new(quic_options.concurrent_connections as usize));
        let mut connection_tasks = Vec::new();
        let requests_left = Arc::new(Mutex::new(quic_options.requests));
        for i in 0..quic_options.concurrent_connections {
            let client = client.clone();
            let server_name = server_name.to_string();
            let requests_left = requests_left.clone();
            let connection_name = (i + b'A') as char;
            let connections_semaphore = connections_semaphore.clone();
            let concurrent_streams = quic_options.concurrent_streams_per_connection;
            connection_tasks.push(async_rt::spawn(async move {
                let _permit = connections_semaphore.acquire().await;
                client::run_connection(
                    client,
                    server_name,
                    server_addr,
                    connection_name.to_string(),
                    requests_left,
                    concurrent_streams,
                    start,
                )
                .await
            }));

            // Wait 1 ms before starting the next connection
            async_rt::time::sleep(Duration::from_millis(1)).await;
        }

        drop(client);

        // Wait for all connections to finish
        let total_connections = connection_tasks.len();
        for task in connection_tasks {
            task.await
                .context("client connection task crashed")?
                .context("client connection errored")?;
        }

        let total_time_sec = start.elapsed().as_secs_f64();
        println!("{:.2}s All connections closed", total_time_sec);

        // Cleanly shut down the server
        let mut handled_connections = 0;
        while let Some(conn_task_handle) = server_handled_connections.next().await {
            conn_task_handle
                .await
                .context("server connection task crashed")?
                .context("server connection task errored")?;

            handled_connections += 1;
            if handled_connections >= total_connections {
                break;
            }
        }
        server.close(VarInt::from_u32(0), b"server shut down");

        Ok(())
    }
}

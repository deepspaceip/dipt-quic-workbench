use crate::config::quinn::QuinnJsonConfig;
use anyhow::Context;
use async_lock::Semaphore;
use fastrand::Rng;
use in_memory_network::async_rt;
use in_memory_network::async_rt::time::Instant;
use in_memory_network::quinn_interop::InMemoryUdpSocket;
use parking_lot::Mutex;
use quinn::Endpoint;
use quinn_proto::crypto::rustls::QuicClientConfig;
use quinn_proto::{ClientConfig, VarInt};
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use std::net::SocketAddr;
use std::sync::Arc;

pub async fn run_connection(
    client: Endpoint,
    server_name: String,
    server_addr: SocketAddr,
    connection_name: String,
    requests_left: Arc<Mutex<u32>>,
    concurrent_streams: u32,
    start: Instant,
) -> anyhow::Result<()> {
    println!(
        "{:.2}s CONNECT (conn = {connection_name})",
        start.elapsed().as_secs_f64()
    );
    let connection = client
        .connect(server_addr, &server_name)
        .context("failed to start connecting to server")?
        .await
        .context("client failed to connect to server")?;
    println!(
        "{:.2}s CONNECTED (conn = {connection_name})",
        start.elapsed().as_secs_f64()
    );

    let requests_semaphore = Arc::new(Semaphore::new(concurrent_streams as usize));
    let mut request_tasks = Vec::new();
    let mut requests_made = 0;
    loop {
        // Break once there are no more requests left to make
        {
            let mut requests_left = requests_left.lock();
            if *requests_left == 0 {
                break;
            }

            *requests_left -= 1;
        }

        let permit = requests_semaphore.clone().acquire_arc().await;
        requests_made += 1;

        // Actually make the request
        let connection = connection.clone();
        let connection_name = connection_name.clone();
        let request_task = async_rt::spawn(async move {
            let request = "GET /index.html";
            println!(
                "{:.2}s {request} (stream = {connection_name}{requests_made})",
                start.elapsed().as_secs_f64()
            );

            let (mut tx, mut rx) = connection.open_bi().await?;
            tx.write_all(request.as_bytes()).await?;
            tx.finish()?;
            tx.stopped().await?;

            rx.read_to_end(usize::MAX).await.with_context(|| {
                format!(
                    "failed to read response from server at {:.2}s",
                    start.elapsed().as_secs_f64()
                )
            })?;

            drop(permit);
            Result::<_, anyhow::Error>::Ok(())
        });

        request_tasks.push(request_task);
    }

    for task in request_tasks {
        task.await
            .context("client stream task crashed")?
            .context("client stream task errored")?;
    }

    println!(
        "{:.2}s DONE (conn = {connection_name}, request/response amount = {requests_made})",
        start.elapsed().as_secs_f64()
    );

    connection.close(VarInt::from_u32(0), &[]);
    Ok(())
}

pub fn client_endpoint(
    server_cert: CertificateDer<'_>,
    client_socket: InMemoryUdpSocket,
    quinn_config: &QuinnJsonConfig,
    quinn_rng: &mut Rng,
    node_id: &str,
) -> anyhow::Result<Endpoint> {
    let mut seed = [0; 32];
    quinn_rng.fill(&mut seed);

    let mut endpoint = Endpoint::new_with_abstract_socket(
        crate::quic::endpoint_config(seed),
        None,
        Arc::new(client_socket),
        async_rt::active_rt(),
    )
    .context("failed to create client endpoint")?;

    endpoint.set_default_client_config(client_config(server_cert, quinn_config, node_id)?);

    Ok(endpoint)
}

fn client_config(
    server_cert: CertificateDer<'_>,
    quinn_config: &QuinnJsonConfig,
    node_id: &str,
) -> anyhow::Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(server_cert)?;

    let default_provider = rustls::crypto::ring::default_provider();
    let provider = rustls::crypto::CryptoProvider {
        cipher_suites: vec![rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256],
        ..default_provider
    };

    let mut crypto = rustls::ClientConfig::builder_with_provider(provider.into())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();

    crypto.key_log = Arc::new(rustls::KeyLogFile::new());

    let mut client_config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto)?));
    client_config.transport_config(Arc::new(crate::quic::transport_config(
        quinn_config,
        node_id,
    )));

    Ok(client_config)
}

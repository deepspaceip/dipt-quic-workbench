//! In-memory network implementation
//!
//! Provides an in-memory network with two peers and an arbitrary number of routers in between

pub mod event;
pub(crate) mod inbound_queue;
pub mod ip;
pub mod link;
pub mod node;
mod outbound_buffer;
pub mod route;
pub mod spec;

use crate::InTransitData;
use crate::async_rt;
use crate::async_rt::time::Instant;
use crate::network::event::{NetworkEventPayload, NetworkEvents};
use crate::network::inbound_queue::InboundQueue;
use crate::network::link::OutgoingPacket;
use crate::network::node::Node;
use crate::network::spec::{NetworkSpec, NodeKind};
use crate::pcap_exporter::PcapExporterFactory;
use crate::quinn_interop::InMemoryUdpSocket;
use crate::tracing::tracer::SimulationStepTracer;
use crate::transmit::OwnedTransmit;
use anyhow::{anyhow, bail};
use fastrand::Rng;
use futures_util::StreamExt;
use link::NetworkLink;
use parking_lot::Mutex;
use route::Route;
use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct PacketArrived {
    pub path: Vec<(Instant, Arc<str>)>,
    pub content: Vec<u8>,
}

/// A network between two hosts, with multiple routers in between
pub struct InMemoryNetwork {
    /// Map from ip addresses to the corresponding nodes
    nodes_by_addr: Arc<HashMap<IpAddr, Arc<Node>>>,
    /// Map from ip addresses to the available route information
    routes_by_addr: Arc<HashMap<IpAddr, Arc<Vec<Route>>>>,
    /// Map from ip address pairs to the corresponding links
    links_by_addr: Arc<HashMap<(IpAddr, IpAddr), Arc<Mutex<NetworkLink>>>>,
    /// Map from ids the corresponding links
    links_by_id: Arc<HashMap<Arc<str>, Arc<Mutex<NetworkLink>>>>,
    pub(crate) tracer: Arc<SimulationStepTracer>,
    rng: Mutex<Rng>,
    next_transmit_number: AtomicU64,
    pcap_exporter_factory: Arc<dyn PcapExporterFactory>,
}

impl InMemoryNetwork {
    /// Initializes a new [`InMemoryNetwork`] based on the provided spec
    pub fn initialize(
        network_spec: NetworkSpec,
        events: NetworkEvents,
        tracer: Arc<SimulationStepTracer>,
        pcap_exporter_factory: Arc<dyn PcapExporterFactory>,
        rng: Rng,
        start: Instant,
        disable_time_warping: bool,
    ) -> anyhow::Result<Arc<Self>> {
        if !disable_time_warping {
            // Time warping is enabled, so the start instant should be zero
            if !tracer.is_fresh() {
                bail!("attempted to initialize network with an old tracer");
            }

            if !start.elapsed().is_zero() {
                bail!("attempted to initialize network with an old start instant");
            }
        }

        let mut routes_by_addr = HashMap::new();
        let all_node_interfaces = network_spec.nodes.iter().map(|n| &n.interfaces);
        for single_node_interfaces in all_node_interfaces {
            for interface in single_node_interfaces {
                for interface_addr in &interface.addresses {
                    let mut routes = interface.routes.clone();
                    routes.sort_by_key(|r| r.cost); // ascending order
                    routes_by_addr.insert(interface_addr.as_ip_addr(), Arc::new(routes));
                }
            }
        }

        let (hosts, routers): (Vec<_>, _) = network_spec
            .nodes
            .into_iter()
            .partition(|n| n.kind == NodeKind::Host);
        if hosts.len() < 2 {
            bail!(
                "Expected at least two hosts in network graph, found {}",
                hosts.len()
            );
        }

        let mut nodes_by_addr = HashMap::new();
        let mut nodes_and_outbound_rx = Vec::new();
        for host in hosts {
            let (h, endpoint, outbound_rx) = Node::host(host)?;
            let h = Arc::new(h);
            let already_existing = nodes_by_addr.insert(endpoint.addr.ip(), h.clone());
            if already_existing.is_some() {
                bail!(
                    "Expected quic endpoints to have unique ip addresses, but at least two endpoints are using {}",
                    endpoint.addr.ip()
                );
            }

            nodes_and_outbound_rx.push((h, outbound_rx));
        }

        let mut links_by_addr = HashMap::new();
        let mut links_by_id = HashMap::new();
        for l in network_spec.links {
            let id = l.id.clone();
            let source = l.source;
            let target = l.target;

            let (l, rx) = NetworkLink::new(l, tracer.clone());
            let l = Arc::new(Mutex::new(l));
            let l_cp = l.clone();
            let conflicting_link = links_by_addr.insert((source, target), l.clone());
            if let Some(conflicting_link) = conflicting_link {
                bail!(
                    "links {} and {} share the same address pair: {} -> {}",
                    id,
                    conflicting_link.lock().id,
                    source,
                    target
                );
            }

            let conflicting_link = links_by_id.insert(id.clone(), l);
            if conflicting_link.is_some() {
                bail!("there is more than one link with id {}", id,);
            }

            async_rt::spawn(async move { process_link_queue(l_cp, rx).await });
        }

        for r in routers {
            let (router, outbound_rx) = Node::router(r)?;
            let router = Arc::new(router);

            let mut inbound_links = HashMap::new();
            for (&(source, target), link) in &links_by_addr {
                if router.addresses.contains(&target) {
                    inbound_links.insert(source, link.clone());
                }
            }

            for &address in &router.addresses {
                let address_taken = nodes_by_addr.insert(address, router.clone());
                if let Some(conflicting_router) = address_taken {
                    bail!(
                        "nodes {} and {} share the same address: {}",
                        router.id,
                        conflicting_router.id,
                        address
                    );
                }
            }

            nodes_and_outbound_rx.push((router, outbound_rx));
        }

        let network = Arc::new(Self {
            nodes_by_addr: Arc::new(nodes_by_addr),
            routes_by_addr: Arc::new(routes_by_addr),
            links_by_addr: Arc::new(links_by_addr),
            links_by_id: Arc::new(links_by_id),
            tracer,
            rng: Mutex::new(rng),
            next_transmit_number: Default::default(),
            pcap_exporter_factory,
        });

        // Process node buffers in the background
        spawn_node_buffer_processors(network.clone(), nodes_and_outbound_rx);

        // Forward packets in the background
        spawn_packet_forwarders(network.clone());

        // Process initial events
        for event in events.initial_events {
            network.process_event(event);
        }

        // Process events in the background
        let network_clone = Arc::downgrade(&network);
        async_rt::spawn(async move {
            for event in events.sorted_events.into_iter() {
                // Wait until next event should run
                async_rt::time::sleep_until(start + event.relative_time).await;

                if let Some(network) = network_clone.upgrade() {
                    network.process_event(event.payload);
                } else {
                    break;
                }
            }

            println!(
                "{:.2}s WARN: no more network events left to process. Did the simulation keep running indefinitely?",
                start.elapsed().as_secs_f64()
            );
        });

        Ok(network)
    }

    fn process_event(&self, event: NetworkEventPayload) {
        let NetworkEventPayload {
            link_id: id,
            status,
            bandwidth_bps,
            delay,
            extra_delay,
            extra_delay_ratio,
            packet_duplication_ratio,
            packet_loss_ratio,
            congestion_event_ratio,
        } = event.clone();

        if bandwidth_bps.is_some() {
            println!("WARN: changing the bandwidth in events is currently unsupported");
        }

        if delay.is_some() {
            println!("WARN: changing the delay in events is currently unsupported");
        }

        if extra_delay.is_some() {
            println!("WARN: changing the extra delay in events is currently unsupported");
        }

        if extra_delay_ratio.is_some() {
            println!("WARN: changing the extra delay ratio in events is currently unsupported");
        }

        if packet_duplication_ratio.is_some() {
            println!(
                "WARN: changing the packet duplication ratio in events is currently unsupported"
            );
        }

        if packet_loss_ratio.is_some() {
            println!("WARN: changing the packet loss ratio in events is currently unsupported");
        }

        if congestion_event_ratio.is_some() {
            println!(
                "WARN: changing the congestion event ratio in events is currently unsupported"
            );
        }

        let Some(link) = self.links_by_id.get(&id) else {
            println!("WARN: skipping received event for link that doesn't exist ({id})");
            return;
        };

        if let Some(status) = status {
            link.lock().update_status(status);
        }

        self.tracer.track_link_event(event);
    }

    pub fn new_packet_id(&self) -> Uuid {
        // We generate or own uuids because we need them to be fully deterministic
        let uuid = self.rng.lock().u128(..);
        Uuid::from_u128(uuid)
    }

    pub fn get_link_status(&self, link_id: &str) -> &'static str {
        self.links_by_id[link_id].lock().status_str()
    }

    pub fn get_link_bandwidth_bps(&self, link_id: &str) -> usize {
        self.links_by_id[link_id].lock().bandwidth_bps
    }

    /// Returns a udp socket for the provided host node
    ///
    /// Note: creating multiple sockets for a single node results in unspecified behavior
    pub fn udp_socket_for_node(self: &Arc<InMemoryNetwork>, node: Arc<Node>) -> InMemoryUdpSocket {
        let pcap_exporter = self
            .pcap_exporter_factory
            .create_pcap_exporter_for_node(&node.id)
            .unwrap();
        InMemoryUdpSocket::from_node(self.clone(), node, pcap_exporter)
    }

    /// Returns the host bound to the provided address
    pub fn host(self: &InMemoryNetwork, ip: IpAddr) -> &Arc<Node> {
        let node = &self.nodes_by_addr[&ip];
        assert!(node.udp_endpoint.is_some(), "Node is not a host: {ip}");
        node
    }

    pub async fn assert_connectivity_between_hosts(
        self: &Arc<Self>,
        host_a: &Arc<Node>,
        host_b: &Arc<Node>,
    ) -> anyhow::Result<(Duration, Duration)> {
        let peers = [(host_a, host_b), (host_b, host_a)];

        // Send 100 packets both ways
        //
        // Note: This ensures that in case of packet loss on the network path the connectivity check
        // still completes.
        for (source, target) in peers {
            for _ in 0..100 {
                let data = self.in_transit_data(
                    source,
                    OwnedTransmit {
                        destination: target.udp_endpoint.as_ref().unwrap().addr,
                        ecn: None,
                        contents: vec![42].into(),
                        segment_size: None,
                    },
                );

                self.forward(source.clone(), data);
            }
        }

        // Wait for 90 days for the packets to arrive
        let days = 90;
        let timeout = Duration::from_secs(3600 * 24 * days);

        // Ensure the packets arrived at each host (one successful delivery is sufficient)
        let a_to_b = async_rt::time::timeout(
            timeout,
            InboundQueue::receive(host_b.udp_endpoint.as_ref().unwrap().inbound.clone(), 1),
        )
        .await;
        let b_to_a = async_rt::time::timeout(
            timeout,
            InboundQueue::receive(host_a.udp_endpoint.as_ref().unwrap().inbound.clone(), 1),
        )
        .await;

        match (a_to_b, b_to_a) {
            (Ok(a_to_b), Ok(b_to_a)) => {
                let stepper = self.tracer.stepper();
                Ok((
                    stepper
                        .get_packet_arrived_at(a_to_b[0].data.id, &host_b.id)
                        .unwrap(),
                    stepper
                        .get_packet_arrived_at(b_to_a[0].data.id, &host_a.id)
                        .unwrap(),
                ))
            }
            (a_to_b, b_to_a) => {
                let report = |failed| if failed { "failed" } else { "succeeded" };
                Err(anyhow!(
                    "failed to deliver packets between the hosts after {days} days (A to B {}, B to A {})",
                    report(a_to_b.is_err()),
                    report(b_to_a.is_err())
                ))
            }
        }
    }

    /// Returns true if there is a route from `node` to `dest`, even if the route is inactive at the
    /// moment because the link is down
    fn has_route_to(&self, node: &Node, dest: IpAddr) -> bool {
        self.walk_links(node, dest, |_link| ControlFlow::Break(true))
            .unwrap_or(false)
    }

    /// Walk links that could potentially route a packet from `node` to `dest`
    ///
    /// Note: this function does not discard links that are down, because in a space setting they
    /// are expected to come up again eventually
    fn walk_links<T>(
        &self,
        node: &Node,
        dest: IpAddr,
        mut walk_fn: impl FnMut(&Arc<Mutex<NetworkLink>>) -> ControlFlow<T>,
    ) -> Option<T> {
        // Prefer direct links if available
        for node_addr in node.addresses() {
            if let Some(link) = self.links_by_addr.get(&(node_addr, dest)) {
                if let ControlFlow::Break(value) = walk_fn(link) {
                    return Some(value);
                }
            }
        }

        // Use routing when no direct links are available
        for node_addr in node.addresses() {
            let routes = &self.routes_by_addr[&node_addr];
            let candidate_links = routes
                .iter()
                .flat_map(|r| r.next_hop_towards_destination(dest))
                .flat_map(|next_hop_addr| self.links_by_addr.get(&(node_addr, next_hop_addr)));

            for link in candidate_links {
                if let ControlFlow::Break(value) = walk_fn(link) {
                    return Some(value);
                }
            }
        }

        None
    }

    pub(crate) fn in_transit_data(&self, source: &Node, transmit: OwnedTransmit) -> InTransitData {
        InTransitData {
            id: self.new_packet_id(),
            duplicate: false,
            source_id: source.id.clone(),
            source_endpoint: source.udp_endpoint.as_ref().unwrap().clone(),
            transmit,
            number: self.next_transmit_number.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// Forwards an [`InTransitData`] to the next node in the network.
    ///
    /// Resolves the link through which the packet should be sent and attempts to send it right
    /// away. If the link is temporarily unavailable or saturated, stores the packet in the node's
    /// buffer for later sending (or drops it when the buffer is full).
    pub(crate) fn forward(
        self: &Arc<InMemoryNetwork>,
        current_node: Arc<Node>,
        data: InTransitData,
    ) {
        self.tracer.track_packet_in_node(&current_node, &data);

        if let Some(udp_endpoint) = &current_node.udp_endpoint {
            if udp_endpoint.addr == data.transmit.destination {
                // The packet has arrived to a quinn endpoint, so we forward it directly to the nodes's
                // inbound queue (from where it will be automatically picked up by quinn)
                udp_endpoint
                    .inbound
                    .clone()
                    .lock()
                    .send(data, Duration::default());

                return;
            }
        }

        // The packet needs to be transmitted to the next hop. We store it in the node's
        // outbound buffer, and it will automatically be picked up by a background task

        let mut randomly_dropped = false;
        let mut duplicate = false;

        let roll = self.rng.lock().f64();
        if roll < current_node.injected_failures.packet_loss_ratio {
            randomly_dropped = true;
        } else if roll
            < current_node.injected_failures.packet_loss_ratio
                + current_node.injected_failures.packet_duplication_ratio
        {
            duplicate = true;
        }

        if randomly_dropped {
            self.tracer.track_dropped_randomly(&data, &current_node);
            return;
        }

        let maybe_duplicate = duplicate.then(|| {
            let mut duplicate_data = data.clone();
            duplicate_data.id = self.new_packet_id();
            duplicate_data.duplicate = true;
            duplicate_data
        });

        current_node.enqueue_outbound(self, data);
        if let Some(duplicate) = maybe_duplicate {
            self.tracer.track_injected_failures(
                &duplicate,
                true,
                Duration::default(),
                false,
                &current_node,
            );

            current_node.enqueue_outbound(self, duplicate);
        }
    }

    fn generate_packet_anomalies(&self, link: &Mutex<NetworkLink>) -> PacketAnomalies {
        let congestion_experienced;
        let mut extra_delay = Duration::from_secs(0);

        // Concurrency: limit the lock guard's lifetime
        {
            let link = link.lock();
            if self.rng.lock().f64() < link.extra_delay_ratio {
                extra_delay = link.extra_delay;
            }

            congestion_experienced = self.rng.lock().f64() < link.congestion_event_ratio;
        }

        PacketAnomalies {
            congestion_experienced,
            extra_delay,
        }
    }
}

pub(crate) struct PacketAnomalies {
    congestion_experienced: bool,
    extra_delay: Duration,
}

fn spawn_node_buffer_processors(
    network: Arc<InMemoryNetwork>,
    nodes: Vec<(
        Arc<Node>,
        futures::channel::mpsc::UnboundedReceiver<InTransitData>,
    )>,
) {
    for (node, outbound_rx) in nodes {
        let network = network.clone();
        async_rt::spawn(async move { process_buffer_for_node(network, node, outbound_rx).await });
    }
}

async fn process_buffer_for_node(
    network: Arc<InMemoryNetwork>,
    node: Arc<Node>,
    mut outbound_rx: futures::channel::mpsc::UnboundedReceiver<InTransitData>,
) {
    while let Some(data) = outbound_rx.next().await {
        if !network.has_route_to(&node, data.transmit.destination.ip()) {
            // Fatal error: there is no route to the destination!
            let nodes = network.tracer.stepper().get_packet_path(data.id);
            let mut path = nodes.join(" -> ");
            path.push_str(" -> ?");

            println!(
                "Fatal network error: missing route to {} ({path})",
                data.transmit.destination
            );
            return;
        }

        let (sent_tx, sent_rx) = tokio::sync::watch::channel(false);

        // Trigger sending through all links, in order of priority (cheaper = better). The first
        // one to complete wins and the rest will discard the packet.
        let mut links = VecDeque::new();
        network.walk_links(&node, data.transmit.destination.ip(), |link| {
            links.push_back(link.clone());
            ControlFlow::Continue::<(), ()>(())
        });

        for link in links.clone() {
            let anomalies = network.generate_packet_anomalies(&link);
            link.lock()
                .outgoing_queue
                .unbounded_send(OutgoingPacket {
                    src_node: node.clone(),
                    data: data.clone(),
                    anomalies,
                    preferred_links: links.clone(),
                    sent_tx: sent_tx.clone(),
                    sent_rx: sent_rx.clone(),
                })
                .expect("the receiver end is active until all senders are dropped");
        }
    }
}

async fn process_link_queue(
    link: Arc<Mutex<NetworkLink>>,
    mut packet_rx: futures::channel::mpsc::UnboundedReceiver<OutgoingPacket>,
) {
    while let Some(mut packet) = packet_rx.next().await {
        if !link.lock().has_bandwidth_available() {
            // Wait until there is enough bandwidth and the link is up. Waiting will be cancelled if
            // the packet gets sent in the meantime.
            NetworkLink::sleep_until_ready_to_send(link.clone(), &mut packet.sent_rx).await;
        }

        while let Some(preferred_link) = packet.preferred_links.pop_front()
            && !Arc::ptr_eq(&link, &preferred_link)
            && !packet.already_sent()
        {
            // We are not the preferred link, so we should give other links a chance to send before
            // us
            tokio::task::yield_now().await;
        }

        // Ready to send
        if !packet.already_sent() {
            packet.sent_tx.send(true).ok();
            link.lock()
                .send(&packet.src_node, packet.data, packet.anomalies);
        }
    }
}

fn spawn_packet_forwarders(network: Arc<InMemoryNetwork>) {
    for link in network.links_by_id.values() {
        let network = network.clone();
        let link = link.clone();
        async_rt::spawn(forward_packets_from_link_to_node(network, link));
    }
}

async fn forward_packets_from_link_to_node(
    network: Arc<InMemoryNetwork>,
    link: Arc<Mutex<NetworkLink>>,
) {
    loop {
        let next_delivered_packets = {
            // Ensure we aren't holding the lock after this block
            let mut lock = link.lock();
            lock.next_delivered_packets(usize::MAX)
        };

        let delivered = next_delivered_packets.await;
        assert!(!delivered.is_empty());

        // Forward the packets that were just delivered
        for transmit in delivered {
            {
                // Only handle the packets if the link didn't go down after sending, otherwise track them as
                // lost
                let link = link.lock();
                if link.was_down_after(transmit.sent) {
                    network.tracer.track_lost_in_transit(&transmit.data, &link);
                    continue;
                }
            }

            let node = &network.nodes_by_addr[&link.lock().target];
            network.forward(node.clone(), transmit.data);
        }
    }
}

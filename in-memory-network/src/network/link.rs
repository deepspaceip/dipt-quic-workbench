use crate::InTransitData;
use crate::async_rt;
use crate::async_rt::time::Instant;
use crate::network::PacketAnomalies;
use crate::network::event::UpdateLinkStatus;
use crate::network::inbound_queue::{InboundQueue, NextPacketDelivery};
use crate::network::node::Node;
use crate::network::spec::NetworkLinkSpec;
use crate::tracing::tracer::SimulationStepTracer;
use async_lock::Semaphore;
use futures_util::future::Shared;
use futures_util::{FutureExt, select_biased};
use parking_lot::Mutex;
use quinn::udp::EcnCodepoint;
use std::collections::VecDeque;
use std::mem;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

pub struct NetworkLink {
    pub id: Arc<str>,
    pub target: IpAddr,
    tracer: Arc<SimulationStepTracer>,
    // Packets currently in flight from the source to the destination
    in_transit: Arc<Mutex<InboundQueue>>,
    // Packets waiting to be sent (i.e., source node up + link up + enough bandwidth)
    pub(crate) outgoing_queue: futures::channel::mpsc::UnboundedSender<OutgoingPacket>,
    pacer: Mutex<PacketPacer>,
    sleep_until_ready_to_send_semaphore: Arc<Semaphore>,
    status: LinkStatus,
    last_down: Option<async_rt::time::Instant>,
    delay: Duration,
    pub(crate) bandwidth_bps: usize,
    pub(crate) congestion_event_ratio: f64,
    pub(crate) extra_delay: Duration,
    pub(crate) extra_delay_ratio: f64,
}

pub(crate) enum LinkStatus {
    Up,
    Down {
        up_tx: futures::channel::oneshot::Sender<()>,
        up_rx: Shared<futures::channel::oneshot::Receiver<()>>,
    },
}

impl LinkStatus {
    pub(crate) fn new_down() -> Self {
        let (up_tx, up_rx) = futures::channel::oneshot::channel();
        LinkStatus::Down {
            up_tx,
            up_rx: up_rx.shared(),
        }
    }

    fn is_down(&self) -> bool {
        match self {
            LinkStatus::Up => false,
            LinkStatus::Down { .. } => true,
        }
    }

    fn notifier_for_link_up(&self) -> Option<Shared<futures::channel::oneshot::Receiver<()>>> {
        match self {
            LinkStatus::Up => None,
            LinkStatus::Down { up_rx, .. } => Some(up_rx.clone()),
        }
    }
}

impl NetworkLink {
    pub(crate) fn new(
        l: NetworkLinkSpec,
        tracer: Arc<SimulationStepTracer>,
    ) -> (
        Self,
        futures::channel::mpsc::UnboundedReceiver<OutgoingPacket>,
    ) {
        let (queue_tx, queue_rx) = futures::channel::mpsc::unbounded();
        let self_ = Self {
            id: l.id,
            status: LinkStatus::Up,
            last_down: None,
            tracer,
            target: l.target,
            in_transit: Arc::new(Mutex::new(InboundQueue::new())),
            outgoing_queue: queue_tx,
            pacer: Mutex::new(PacketPacer::new(l.bandwidth_bps)),
            sleep_until_ready_to_send_semaphore: Arc::new(Semaphore::new(1)),
            delay: l.delay,
            bandwidth_bps: l.bandwidth_bps as usize,
            congestion_event_ratio: l.congestion_event_ratio,
            extra_delay: l.extra_delay,
            extra_delay_ratio: l.extra_delay_ratio,
        };

        (self_, queue_rx)
    }

    pub(crate) fn was_down_after(&self, instant: Instant) -> bool {
        matches!(self.last_down, Some(down) if down > instant)
    }

    pub(crate) fn status_str(&self) -> &'static str {
        match self.status {
            LinkStatus::Up => "UP",
            LinkStatus::Down { .. } => "DOWN",
        }
    }

    pub(crate) fn update_status(&mut self, update: UpdateLinkStatus) {
        let status = mem::replace(&mut self.status, LinkStatus::Up);
        match (status, update) {
            (status @ LinkStatus::Down { .. }, UpdateLinkStatus::Down)
            | (status @ LinkStatus::Up, UpdateLinkStatus::Up) => {
                // No update, restore original status
                self.status = status;
            }

            (LinkStatus::Up, UpdateLinkStatus::Down) => {
                // Set status to down
                self.status = LinkStatus::new_down();
                self.last_down = Some(Instant::now());

                // Nothing else to do here, because:
                // 1. already sent packets will be dropped by the forwarding code if they are still in flight
                // 2. packets in the router's outbound buffer will stay there until the link is back up
                // 3. attempting to send new packets will cause them to land in the buffer (if there's space)
            }

            (LinkStatus::Down { up_tx, .. }, UpdateLinkStatus::Up) => {
                // Set status to up and notify anyone waiting that the link is back up
                self.status = LinkStatus::Up;
                up_tx.send(()).ok();
            }
        }
    }

    pub(crate) fn send(
        &mut self,
        src_node: &Node,
        mut data: InTransitData,
        anomalies: PacketAnomalies,
    ) {
        // Sanity checks
        assert!(self.pacer.lock().can_send(Instant::now()));
        assert!(matches!(self.status, LinkStatus::Up));

        // Apply "congestion experience" anomaly if requested
        if anomalies.congestion_experienced {
            // Sanity check: the Quinn-provided transmit must indicate support for ECN
            assert!(
                data.transmit
                    .ecn
                    .is_some_and(|codepoint| codepoint as u8 == 0b10 || codepoint as u8 == 0b01)
            );

            data.transmit.ecn = Some(EcnCodepoint::from_bits(0b11).unwrap())
        }

        // Trace
        self.tracer.track_packet_in_transit(src_node, self, &data);

        // Send
        // Track send on link and get virtual completion time
        let tx_completion_time = self.pacer.lock().track_send(Instant::now(), data.transmit.packet_size());
        // Release buffer
        src_node
            .outbound_buffer()
            .release(data.transmit.packet_size());

        // Pass the completion time as the "sent" timestamp to inbound queue
        self.in_transit
            .lock()
            .send( data, tx_completion_time,self.delay + anomalies.extra_delay);
    }

    pub(crate) async fn sleep_until_ready_to_send(
        src_node: Arc<Node>,
        this: Arc<Mutex<Self>>,
        packet_sent_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) {
        assert!(
            !this.lock().has_bandwidth_available(),
            "we should only wait when no bandwidth is available"
        );

        // Ensure this method is never executed concurrently, to prevent two callers from waiting
        // at the same time and thinking they are both allowed to send at the end
        let semaphore = this.lock().sleep_until_ready_to_send_semaphore.clone();
        let _permit = semaphore.acquire().await;

        let duration_until_enough_bandwidth = this
            .lock()
            .pacer
            .lock()
            .duration_until_can_send(Instant::now());

        // Sleep until enough bandwidth or until the packet gets sent, whichever comes first
        select_biased! {
            _ = packet_sent_rx.changed().fuse() => return,
            _ = async_rt::time::sleep(duration_until_enough_bandwidth).fuse() => {}
        }

        loop {
            // Sleep until the link is up, if necessary
            let notifier_for_link_up = this.lock().status.notifier_for_link_up();
            if let Some(notifier_for_link_up) = notifier_for_link_up {
                select_biased! {
                    // Break the sleep if someone else sends the packet in the meantime
                    _ = packet_sent_rx.changed().fuse() => return,
                    _ = notifier_for_link_up.fuse() => {}
                }
            }

            // Sleep until the node is up, if necessary
            // Assumption: a link is always sending from the same node, so we are not blocking other
            // nodes here
            let notifier_for_node_up = src_node.status.lock().notifier_for_node_up();
            if let Some(notifier_for_node_up) = notifier_for_node_up {
                select_biased! {
                    // Break the sleep if someone else sends the packet in the meantime
                    _ = packet_sent_rx.changed().fuse() => return,
                    _ = notifier_for_node_up.fuse() => {}
                }
            }

            // Important: the link could have gone down again while we waited on the node to come up
            if let LinkStatus::Up = this.lock().status {
                // Link is still up, ready to send!
                return;
            }
        }
    }

    pub(crate) fn has_bandwidth_available(&mut self) -> bool {
        // concurrency: note the line below acquires a permit, but drops it right away
        let packets_are_waiting_for_bandwidth = self
            .sleep_until_ready_to_send_semaphore
            .try_acquire()
            .is_none();

        if packets_are_waiting_for_bandwidth || self.status.is_down() {
            return false;
        }

        let now = Instant::now();
        let duration = self.pacer.lock().duration_until_can_send(now);
        duration.is_zero()
    }

    pub(crate) fn next_delivered_packets(&mut self, max_transmits: usize) -> NextPacketDelivery {
        NextPacketDelivery::new(self.in_transit.clone(), max_transmits)
    }
}

// Ensures that only a single packet at a time is being sent
struct PacketPacer {
    bandwidth_bps: f64,
    last_send: Option<SendingPacket>,
}

#[derive(Clone)]
struct SendingPacket {
    send_done: Instant,
}

impl PacketPacer {
    fn new(bandwidth_bps: u64) -> Self {
        Self {
            bandwidth_bps: bandwidth_bps as f64,
            last_send: None,
        }
    }

    fn can_send(&mut self, now: Instant) -> bool {
        // Allow sending if the debt is less than 1ms
        self.duration_until_can_send(now).is_zero()
    }

    fn duration_until_can_send(&self, now: Instant) -> Duration {
        match &self.last_send {
            None => Duration::default(),
            Some(p) => {
                let debt = p
                    .send_done
                    .into_std()
                    .saturating_duration_since(now.into_std());
                let threshold = Duration::from_millis(1);

                if debt < threshold {
                    Duration::default()
                } else {
                    // Sleep for the excess beyond the 1ms buffer.
                    // This ensures the link stays saturated during the sleep.
                    debt.saturating_sub(threshold)
                }
            }
        }
    }

    fn track_send(&mut self, now: Instant, packet_size_bytes: usize) -> Instant {
        let packet_size_bits = packet_size_bytes.saturating_mul(8);
        // Calculate transmission duration with nanosecond precision for high accuracy
        let send_duration_ns =
            packet_size_bits as f64 / self.bandwidth_bps * 1_000_000_000.0;
        let duration = Duration::from_nanos(send_duration_ns.ceil() as u64);

        // Determine the start time for this packet:
        // If the link was idle, start now.
        // If the link is still busy (send_done > now), start when the previous packet finishes.
        let start_time = match &self.last_send {
            Some(last) if last.send_done > now => last.send_done,
            _ => now,
        };

        let completion_time = start_time + duration;
        self.last_send = Some(SendingPacket {
            send_done: completion_time,
        });

        completion_time
    }
}

pub(crate) struct OutgoingPacket {
    pub(crate) in_node_since: Instant,
    pub(crate) src_node: Arc<Node>,
    pub(crate) data: InTransitData,
    pub(crate) anomalies: PacketAnomalies,
    pub(crate) preferred_links: VecDeque<Arc<Mutex<NetworkLink>>>,
    pub(crate) handled_tx: tokio::sync::watch::Sender<bool>,
    pub(crate) handled_rx: tokio::sync::watch::Receiver<bool>,
}

#[derive(Clone)]
pub(crate) struct BufferedPacket {
    pub(crate) in_node_since: Instant,
    pub(crate) data: InTransitData,
}

impl OutgoingPacket {
    pub(crate) fn already_handled(&self) -> bool {
        *self.handled_rx.borrow()
    }
}

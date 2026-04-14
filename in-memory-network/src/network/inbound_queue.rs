use crate::InTransitData;
use crate::async_rt;
use crate::async_rt::time::Instant;
use parking_lot::Mutex;
use std::collections::BinaryHeap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker, ready};
use std::time::Duration;

pub struct InboundQueue {
    queue: BinaryHeap<PrioritizedInTransitData>,
    notify_new_transmits: Vec<Waker>,
}

impl InboundQueue {
    pub(crate) fn new() -> Self {
        Self {
            queue: BinaryHeap::new(),
            notify_new_transmits: Vec::new(),
        }
    }

    pub(crate) fn send(&mut self, data: InTransitData, sent: Instant, delay: Duration) {
        self.queue.push(PrioritizedInTransitData {
            data,
            sent,
            delay,
        });

        for waker in self.notify_new_transmits.drain(..) {
            waker.wake();
        }
    }

    pub(crate) fn receive(this: Arc<Mutex<Self>>, max_transmits: usize) -> NextPacketDelivery {
        NextPacketDelivery::new(this.clone(), max_transmits)
    }
}

pub struct NextPacketDelivery {
    sleep: Option<Pin<Box<async_rt::Timer>>>,
    queue: Arc<Mutex<InboundQueue>>,
    max_transmits: usize,
}

impl NextPacketDelivery {
    pub fn new(queue: Arc<Mutex<InboundQueue>>, max_transmits: usize) -> Self {
        Self {
            sleep: None,
            queue,
            max_transmits,
        }
    }
}

impl Future for NextPacketDelivery {
    type Output = Vec<DeliveredTransmit>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut delivered = Vec::new();
        loop {
            if delivered.len() >= self.max_transmits {
                break;
            }

            if let Some(sleep) = &mut self.sleep {
                // Instead of sleeping, we return the number of delivered packets we have found so
                // the reader can make progress
                if !delivered.is_empty() {
                    break;
                }

                ready!(sleep.as_mut().poll(cx));

                // Sleep has elapsed, so let's get rid of it
                self.sleep = None;
            }

            let Some(next_arrival_time) = self
                .queue
                .lock()
                .queue
                .peek()
                .map(|next| next.arrival_time())
            else {
                // No enqueued packets, so nothing to do
                break;
            };

            if next_arrival_time > Instant::now() {
                // Sleep in the next iteration until we are allowed to deliver the next packet
                self.sleep = Some(Box::pin(async_rt::time::sleep_until(next_arrival_time)));
            } else {
                // Deliver the next packet
                let data = self.queue.lock().queue.pop().unwrap();
                delivered.push(DeliveredTransmit {
                    data: data.data,
                    sent: data.sent,
                });
            }
        }

        if delivered.is_empty() {
            self.queue
                .lock()
                .notify_new_transmits
                .push(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(delivered)
        }
    }
}

pub struct DeliveredTransmit {
    pub(crate) data: InTransitData,
    pub(crate) sent: Instant,
}

// In transit data, sorted by arrival time
struct PrioritizedInTransitData {
    data: InTransitData,
    sent: Instant,
    delay: Duration,
}

impl PrioritizedInTransitData {
    fn arrival_time(&self) -> Instant {
        self.sent + self.delay
    }
}

impl Eq for PrioritizedInTransitData {}

impl PartialEq<Self> for PrioritizedInTransitData {
    fn eq(&self, other: &Self) -> bool {
        self.arrival_time() == other.arrival_time() && self.data.number == other.data.number
    }
}

impl PartialOrd<Self> for PrioritizedInTransitData {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrioritizedInTransitData {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Note: the order is reversed, so the "max" in transit data will be the next one to be sent
        other
            .arrival_time()
            .cmp(&self.arrival_time())
            .then(other.data.number.cmp(&self.data.number))
    }
}

use serde::Deserialize;

#[derive(Deserialize, Clone, Copy, Debug)]
#[serde(rename_all = "snake_case")]
pub enum CongestionControlAlgorithm {
    /// Cubic congestion control (Quinn default)
    Cubic,
    /// NewReno congestion control
    NewReno,
    /// Disables congestion control and uses the intial_congestion_window as a fixed window instead
    NoCc,
    /// Configures congestion control to use a variant of `NewReno` that ignores packet
    /// loss and only takes ECN into consideration.
    EcnReno,
}

#[derive(Deserialize, Clone)]
pub struct QuinnJsonConfig {
    /// The initial RTT of the QUIC connection, in milliseconds (used before an RTT sample is
    /// available).
    ///
    /// For delay-tolerant networking, it is recommended to set this to a value slightly higher than
    /// the real RTT. If the value is too low, there will be needless retransmissions of packets
    /// until the endpoint is able to infer the real RTT.
    pub initial_rtt_ms: u64,
    /// The maximum idle timeout of the QUIC connection, in milliseconds.
    ///
    /// When expecting a continuous exchange of information, a small idle timeout helps to detect
    /// connection loss. In delay-tolerant networking, it is useful to use a very high timeout, to
    /// ensure the connection never gets lost due to unexpected delays.
    pub maximum_idle_timeout_ms: u64,
    /// Maximum reordering in packet numbers before considering a packet lost. Should not be less
    /// than 3, per RFC5681.
    pub packet_threshold: u32,
    /// Whether MTU discovery should be enabled
    pub mtu_discovery: bool,
    /// Whether the send and receive windows should be maximized, allowing an unbounded number of
    /// unacknowledged in-flight packets
    pub maximize_send_and_receive_windows: bool,
    /// The number of ACK-eliciting packets an endpoint may receive without immediately sending an
    /// ACK.
    ///
    /// Setting this threshold to a high value is particularly useful when we expect to receive long
    /// streams of information from the server, without sending anything back from the client.
    pub ack_eliciting_threshold: u32,
    /// The maximum amount of time that an endpoint waits before sending an ACK when the
    /// ACK-eliciting threshold hasn't been reached.
    ///
    /// Setting this to a high value is particularly useful in combination with a high ACK-eliciting
    /// threshold.
    pub max_ack_delay_ms: u64,
    /// Which congestion control algorithm to use
    pub congestion_controller: CongestionControlAlgorithm,
    /// The initial congestion window size in multiples of base datagram size. If missing the algorithm's
    /// default is used.
    /// For 'NoCc', this value is used as the fixed, constant window. If missing it defaults to u64::MAX.
    pub initial_congestion_window_packets: Option<u64>,
}

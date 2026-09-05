use serde::{Deserialize, Serialize};

// Wire formats for libp2p Request/Response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClipboardRequest {
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClipboardResponse {
    pub ack: bool,
}

// Events emitted FROM the network TO the frontend
#[derive(Debug, Clone)]
pub enum NetworkEvent {
    ListeningOn(String),
    PeerDiscovered(String),
    PeerExpired(String),
    PeerConnected(String),
    PeerDisconnected(String),
    PeerUnreachable(String),
    MessageSent { text: String },
    MessageReceived { from: String, text: String },
    NetworkError { peer: String, error: String },
}

// Commands sent FROM the frontend to the network
#[derive(Debug, Clone)]
pub enum NetworkCommand {
    BroadcastText(String),
    SendTextTo {
        target_peer_id: String,
        text: String,
    },
}

use crate::core::queue::ClipboardItem;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Wire formats for libp2p Request/Response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClipboardRequest {
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClipboardResponse {
    pub ack: bool,
    pub reason: Option<String>,
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
    MessageSent {
        text: String,
    },
    MessageReceived {
        from: String,
        text: String,
        queue_was_empty: bool,
        queue_len: usize,
        queue_bytes: usize,
    },
    DeliveryFailed {
        peer: String,
        reason: String,
    },
    QueueRejected {
        reason: String,
    },
    QueueItemDismissed {
        dismissed: ClipboardItem,
        next: Option<ClipboardItem>,
    },
    QueueItemStarred {
        starred: ClipboardItem, // a.k.a dismissed but starred
        next: Option<ClipboardItem>,
    },
    QueuePendingList {
        items: Vec<ClipboardItem>,
    },
    QueueStarredList {
        items: Vec<ClipboardItem>,
    },
    QueueEmpty,
    NetworkError {
        peer: String,
        error: String,
    },
    List {
        listen_addresses: Vec<String>,
        discovered_peers: HashMap<String, Vec<String>>,
        connected_peers: Vec<String>,
    },
}

// Commands sent FROM the frontend to the network
#[derive(Debug, Clone)]
pub enum NetworkCommand {
    BroadcastText(String),
    SendTextTo {
        target_peer_id: String,
        text: String,
    },
    DismissCurrent,
    StarCurrent,
    ListPending,
    ListStarred,
    List,
}

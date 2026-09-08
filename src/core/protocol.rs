use crate::core::queue::ClipboardItem;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct StarredMetadata {
    pub id: u128,
    pub preview: String,
    pub size_bytes: usize,
    pub from: Option<String>,
}

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
    QueueItemUnstarred {
        id: u128,
    },
    StarredPageLoaded {
        items: Vec<StarredMetadata>,
        offset: usize,
    },
    StarredTextLoaded {
        id: u128,
        text: String,
    },
    DatabaseError(String),
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
    Unstar(u128),
    LoadStarredPage {
        limit: usize,
        offset: usize,
    },
    GetFullStarredText(u128),
    List,
}

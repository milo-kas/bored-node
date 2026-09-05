pub mod core;

use crate::core::network::run_network_loop;
pub use crate::core::protocol::{NetworkCommand, NetworkEvent};
use std::error::Error;
use tokio::sync::mpsc;

pub struct Node {
    command_tx: mpsc::Sender<NetworkCommand>,
    pub event_rx: mpsc::Receiver<NetworkEvent>,
}

impl Node {
    /// Spawns the node on a background Tokio task
    pub async fn start() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let (command_tx, command_rx) = mpsc::channel(64);
        let (event_tx, event_rx) = mpsc::channel(64);

        tokio::spawn(async move {
            if let Err(err) = run_network_loop(command_rx, event_tx).await {
                eprintln!("Network loop terminated: {err:?}");
            }
        });

        Ok(Self {
            command_tx,
            event_rx,
        })
    }

    /// Direct broadcast to all peers discovered on the LAN
    pub async fn broadcast_text(&self, text: String) -> Result<(), Box<dyn Error>> {
        self.command_tx
            .send(NetworkCommand::BroadcastText(text))
            .await
            .map_err(|e| e.into())
    }

    /// Direct transmission to a specific peer
    pub async fn send_text_to(&self, peer_id: String, text: String) -> Result<(), Box<dyn Error>> {
        self.command_tx
            .send(NetworkCommand::SendTextTo {
                target_peer_id: peer_id,
                text,
            })
            .await
            .map_err(|e| e.into())
    }
}

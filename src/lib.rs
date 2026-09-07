pub mod core;

use crate::core::network::run_network_loop;
pub use crate::core::protocol::{NetworkCommand, NetworkEvent};
pub use crate::core::queue::{ClipboardItem, ClipboardQueue};
use std::error::Error;
use tokio::sync::mpsc;

pub struct Node {
    command_tx: mpsc::Sender<NetworkCommand>,
    pub event_rx: mpsc::Receiver<NetworkEvent>,
}

impl Node {
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

    pub async fn broadcast_text(&self, text: String) -> Result<(), Box<dyn Error>> {
        self.command_tx
            .send(NetworkCommand::BroadcastText(text))
            .await
            .map_err(|e| e.into())
    }

    pub async fn send_text_to(&self, peer_id: String, text: String) -> Result<(), Box<dyn Error>> {
        self.command_tx
            .send(NetworkCommand::SendTextTo {
                target_peer_id: peer_id,
                text,
            })
            .await
            .map_err(|e| e.into())
    }

    pub async fn dismiss_current(&self) -> Result<(), Box<dyn Error>> {
        self.command_tx
            .send(NetworkCommand::DismissCurrent)
            .await
            .map_err(|e| e.into())
    }

    pub async fn star_current(&self) -> Result<(), Box<dyn Error>> {
        self.command_tx
            .send(NetworkCommand::StarCurrent)
            .await
            .map_err(|e| e.into())
    }

    pub async fn list_pending(&self) -> Result<(), Box<dyn Error>> {
        self.command_tx
            .send(NetworkCommand::ListPending)
            .await
            .map_err(|e| e.into())
    }

    pub async fn list_starred(&self) -> Result<(), Box<dyn Error>> {
        self.command_tx
            .send(NetworkCommand::ListStarred)
            .await
            .map_err(|e| e.into())
    }

    pub async fn list(&self) -> Result<(), Box<dyn Error>> {
        self.command_tx
            .send(NetworkCommand::List)
            .await
            .map_err(|e| e.into())
    }
}

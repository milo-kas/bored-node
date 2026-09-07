pub mod core;

use crate::core::network::run_network_loop;
pub use crate::core::protocol::{NetworkCommand, NetworkEvent};
pub use crate::core::queue::{ClipboardItem, ClipboardQueue};
use std::error::Error;
use std::path::PathBuf;
use tokio::sync::mpsc;

pub struct Node {
    command_tx: mpsc::Sender<NetworkCommand>,
    pub event_rx: mpsc::Receiver<NetworkEvent>,
}

impl Node {
    pub async fn start(custom_db_path: Option<PathBuf>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let (command_tx, command_rx) = mpsc::channel(64);
        let (event_tx, event_rx) = mpsc::channel(64);
        let db_path = match custom_db_path {
            Some(path) => path,
            None => {
                #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
                {
                    let base_dir = dirs::data_local_dir().expect("Failed to resolve local data directory");
                    let app_dir = base_dir.join("bored-node");
                    std::fs::create_dir_all(&app_dir)?;
                    app_dir.join("starred.db")
                }
                #[cfg(any(target_os = "android", target_os = "ios"))]
                {
                    panic!("custom_db_path is required on mobile targets");
                }
            }
        };

        tokio::spawn(async move {
            if let Err(err) = run_network_loop(command_rx, event_tx, db_path).await {
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
            .send(NetworkCommand::LoadStarredPage {
                limit: 50,
                offset: 0,
            })
            .await
            .map_err(|e| e.into())
    }

    pub async fn unstar(&self, id: u128) -> Result<(), Box<dyn Error>> {
        self.command_tx
            .send(NetworkCommand::Unstar(id))
            .await
            .map_err(|e| e.into())
    }

    pub async fn load_starred_page(&self, limit: usize, offset: usize) -> Result<(), Box<dyn Error>> {
        self.command_tx
            .send(NetworkCommand::LoadStarredPage { limit, offset })
            .await
            .map_err(|e| e.into())
    }

    pub async fn get_full_starred_text(&self, id: u128) -> Result<(), Box<dyn Error>> {
        self.command_tx
            .send(NetworkCommand::GetFullStarredText(id))
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

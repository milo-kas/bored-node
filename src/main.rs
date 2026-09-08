use bored_node::{NetworkEvent, Node};
use dirs;
use dirs::data_local_dir;
use std::{error::Error, path::PathBuf};
use tokio::io::{self, AsyncBufReadExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let db_path = resolve_db_path();

    let mut node = Node::start(Some(db_path)).await?;
    let mut stdin = io::BufReader::new(io::stdin()).lines();

    println!("bored-node CLI running.");
    println!("- Type a message and press Enter to broadcast.");
    println!("- Type '/to <peer_id> <message>' to send to a specific peer.");
    println!(
        "- Use '/queue next', '/queue star', '/queue all', '/list star', and '/unstar <id>'.\n"
    );

    loop {
        tokio::select! {
            result = stdin.next_line() => match result {
                Ok(Some(line)) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    if let Some(rest) = trimmed.strip_prefix("/to ") {
                        let mut parts = rest.splitn(2, ' ');
                        if let (Some(target), Some(msg)) = (parts.next(), parts.next()) {
                            println!("Sending to {target}...");
                            let _ = node.send_text_to(target.to_string(), msg.to_string()).await;
                        } else {
                            println!("Usage: /to <peer_id> <message>");
                        }
                    } else if trimmed == "/list" {
                        let _ = node.list().await;
                    } else if trimmed == "/queue next" {
                        let _ = node.dismiss_current().await;
                    } else if trimmed == "/queue star" {
                        let _ = node.star_current().await;
                    } else if trimmed == "/queue all" {
                        let _ = node.list_pending().await;
                    } else if trimmed == "/list star" {
                        let _ = node.list_starred().await;
                    } else if let Some(id_str) = trimmed.strip_prefix("/unstar ") {
                        match id_str.trim().parse::<u128>() {
                            Ok(id) => {
                                let _ = node.unstar(id).await;
                            }
                            Err(_) => {
                                println!("Usage: /unstar <id>");
                            }
                        }
                    } else {
                        let _ = node.broadcast_text(trimmed.to_string()).await;
                    }
                }
                Ok(None) => {
                    println!("\nstdin closed; exiting.");
                    break;
                }
                Err(err) => {
                    eprintln!("stdin error: {err}");
                    break;
                }
            },

            Some(event) = node.event_rx.recv() => match event {
                NetworkEvent::ListeningOn(addr) => println!("[INFO] Listening on {addr}"),
                NetworkEvent::PeerDiscovered(peer) => println!("[DISCOVERY] Found peer: {peer}"),
                NetworkEvent::PeerExpired(peer) => println!("[DISCOVERY] Lost peer: {peer}"),
                NetworkEvent::PeerConnected(peer) => println!("[CONNECTION] Connected to {peer}"),
                NetworkEvent::PeerDisconnected(peer) => println!("[CONNECTION] Disconnected from {peer}"),
                NetworkEvent::PeerUnreachable(peer) => println!("[INFO] Peer {peer} is unreachable. Pruned from routing table."),
                NetworkEvent::MessageSent { text } => {
                    println!("\n--- CLIPBOARD SENT ---");
                    println!("{text}");
                    println!("--------------------------\n");
                }
                NetworkEvent::MessageReceived {
                    from,
                    text,
                    queue_was_empty,
                    queue_len,
                    queue_bytes,
                } => {
                    if queue_was_empty {
                        print_received_block(Some(&from), &text);
                    } else {
                        println!(
                            "[INFO] New item received from {from}. ({}) (/queue next to view)",
                            format_queue_status(queue_len, queue_bytes)
                        );
                    }
                }
                NetworkEvent::DeliveryFailed { peer, reason } => {
                    println!("[ERROR] Delivery failed to {peer}: {reason}");
                }
                NetworkEvent::QueueRejected { reason } => {
                    println!("[ERROR] Incoming item rejected: {reason}");
                }
                NetworkEvent::QueueItemDismissed { dismissed, next } => {
                    println!("[INFO] Dismissed: {}", preview_text(&dismissed.text, 40));
                    if let Some(next_item) = next {
                        print_received_block(next_item.from.as_deref(), &next_item.text);
                    } else {
                        println!("Queue is now empty.")
                    }
                }
                NetworkEvent::QueueItemStarred { starred, next } => {
                    println!("[INFO] Starred: {}", preview_text(&starred.text, 40));
                    if let Some(next_item) = next {
                        print_received_block(next_item.from.as_deref(), &next_item.text);
                    } else {
                        println!("Queue is now empty.")
                    }
                }
                NetworkEvent::QueuePendingList { items } => {
                    if items.is_empty() {
                        println!("Queue is empty.");
                    } else {
                        for item in items {
                            println!("[{}] {}", item.id, preview_text(&item.text, 80));
                        }
                    }
                }
                NetworkEvent::StarredPageLoaded { items, .. } => {
                    if items.is_empty() {
                        println!("No starred items.");
                    } else {
                        for item in items {
                            println!("--- STARRED ITEM [{}] ---", item.id);
                            println!("{}", item.preview);
                            println!("--------------------");
                        }
                    }
                }
                NetworkEvent::QueueItemUnstarred { id } => {
                    println!("[INFO] Unstarred [{id}]");
                }
                NetworkEvent::StarredTextLoaded { id, text } => {
                    println!("--- STARRED ITEM [{id}] ---");
                    println!("{text}");
                    println!("--------------------");
                }
                NetworkEvent::DatabaseError(error) => {
                    println!("[ERROR] Database: {error}");
                }
                NetworkEvent::QueueEmpty => println!("Queue is empty."),
                NetworkEvent::List { listen_addresses, discovered_peers, connected_peers } => {
                    println!("\n---LIST CURRENT NODE STATUS---");
                    println!("Listening on:");
                    for addr in listen_addresses {
                        println!("> {addr}");
                    }
                    println!("\nDiscovered peers:");
                    for (peer, addrs) in discovered_peers {
                        println!("> {peer}:");
                        for addr in addrs {
                            println!(">>{addr}");
                        }
                    }
                    println!("\nConnected peers:");
                    for peer in connected_peers {
                        println!("> {peer}");
                    }
                    println!("--------------------------\n");
                }
                NetworkEvent::NetworkError { peer, error } => {
                    println!("[ERROR] Network issue with {peer}: {error}");
                }
            }
        }
    }

    Ok(())
}

fn preview_text(text: &str, max_len: usize) -> String {
    if text.chars().count() <= max_len {
        text.to_string()
    } else {
        let mut preview = text.chars().take(max_len).collect::<String>();
        preview.push_str("...");
        preview
    }
}

fn print_received_block(from: Option<&str>, text: &str) {
    println!("\n--- CLIPBOARD RECEIVED ---");
    if let Some(from) = from {
        println!("From: {from}");
    }
    println!("{text}");
    println!("--------------------------\n");
}

fn format_queue_status(queue_len: usize, queue_bytes: usize) -> String {
    let mb = queue_bytes as f64 / 1_000_000.0;
    format!("queue {queue_len}/20, {:.2}MB/400MB", mb)
}

/// Resolve the SQLite DB path using a platform fallback chain
fn resolve_db_path() -> PathBuf {
    let db_path = data_local_dir()
        // Standard OS local data dir
        .map(|dir| dir.join("bored-node").join("starred.db"))
        // Minimal Unix
        .or_else(|| {
            std::env::var("HOME").ok().map(|home| {
                PathBuf::from(home)
                    .join(".local")
                    .join("share")
                    .join("bored-node")
                    .join("starred.db")
            })
        })
        // Current working dir (last resort)
        .unwrap_or_else(|| PathBuf::from("starred.db"));

    if let Some(parent) = db_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    db_path
}

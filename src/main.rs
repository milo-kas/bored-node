use bored_node::{NetworkEvent, Node};
use std::error::Error;
use tokio::io::{self, AsyncBufReadExt};

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut node = Node::start().await?;
    let mut stdin = io::BufReader::new(io::stdin()).lines();

    println!("bored-node CLI running.");
    println!("- Type a message and press Enter to broadcast.");
    println!("- Type '/to <peer_id> <message>' to send to a specific peer.");
    println!("- Use '/queue next', '/queue star', '/queue all', and '/list star'.\n");

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
                NetworkEvent::QueueItemDismissed { item } => {
                    print_received_block(item.from.as_deref(), &item.text);
                }
                NetworkEvent::QueueItemStarred { item } => {
                    println!("[INFO] Item saved to starred list.");
                    print_received_block(item.from.as_deref(), &item.text);
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
                NetworkEvent::QueueStarredList { items } => {
                    if items.is_empty() {
                        println!("No starred items.");
                    } else {
                        for item in items {
                            println!("--- STARRED ITEM ---");
                            println!("{}", item.text);
                            println!("--------------------");
                        }
                    }
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

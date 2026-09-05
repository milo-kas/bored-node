use bored_node::{NetworkEvent, Node};
use std::error::Error;
use tokio::io::{self, AsyncBufReadExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut node = Node::start().await?;
    let mut stdin = io::BufReader::new(io::stdin()).lines();

    println!("bored-node CLI running.");
    println!("- Type a message and press Enter to broadcast.");
    println!("- Type '/to <peer_id> <message>' to send to a specific peer.\n");

    loop {
        tokio::select! {
            // Read a line from stdin and forward it to the network layer.
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

            // Handle asynchronous events coming back from the network loop.
            Some(event) = node.event_rx.recv() => match event {
                NetworkEvent::ListeningOn(addr) => println!("[INFO] Listening on {addr}"),
                NetworkEvent::PeerDiscovered(peer) => println!("[DISCOVERY] Found peer: {peer}"),
                NetworkEvent::PeerExpired(peer) => println!("[DISCOVERY] Lost peer: {peer}"),
                NetworkEvent::PeerConnected(peer) => println!("[CONNECTION] Connected to {peer}"),
                NetworkEvent::PeerDisconnected(peer) => println!("[CONNECTION] Disconnected from {peer}"),
                NetworkEvent::MessageSent { text } => {
                    println!("\n--- CLIPBOARD SENT ---");
                    println!("{text}");
                    println!("--------------------------\n");
                }
                NetworkEvent::MessageReceived { from, text } => {
                    println!("\n--- CLIPBOARD RECEIVED ---");
                    println!("Node: {from}");
                    println!("{text}");
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

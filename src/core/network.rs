use super::{
    db::DbCommand,
    protocol::{ClipboardRequest, ClipboardResponse, NetworkCommand, NetworkEvent},
    queue::{ClipboardQueue, QueueError},
};
use futures::StreamExt;
use libp2p::{
    PeerId, StreamProtocol, identity, mdns,
    ping,
    request_response::{self, ProtocolSupport},
    swarm::{NetworkBehaviour, SwarmEvent},
};
use std::{
    collections::HashSet,
    error::Error,
    fmt::Display,
    path::PathBuf,
    str::FromStr,
    time::Duration,
};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

#[derive(NetworkBehaviour)]
pub struct BoredBehaviour {
    pub req_res: request_response::cbor::Behaviour<ClipboardRequest, ClipboardResponse>,
    pub mdns: mdns::tokio::Behaviour,
    pub ping: ping::Behaviour,
}

fn emit_event(event_tx: &mpsc::Sender<NetworkEvent>, event: NetworkEvent) {
    if let Err(mpsc::error::TrySendError::Full(_)) = event_tx.try_send(event) {
        eprintln!("[WARNING] Event channel buffer full (64). UI is lagging; event dropped.");
    }
}

pub async fn run_network_loop(
    mut command_rx: mpsc::Receiver<NetworkCommand>,
    event_tx: mpsc::Sender<NetworkEvent>,
    db_path: PathBuf,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let db_tx = super::db::spawn_db_actor(db_path);

    let local_key = identity::Keypair::generate_ed25519();

    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(|key| {
            let protocols = [(
                StreamProtocol::new("/bored-node/clipboard/1.0.0"),
                ProtocolSupport::Full,
            )];
            let req_res = request_response::cbor::Behaviour::new(
                protocols,
                request_response::Config::default(),
            );
            let mdns =
                mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;
            let ping = ping::Behaviour::default();
            Ok(BoredBehaviour {
                req_res,
                mdns,
                ping,
            })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(86400)))
        .build();

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    let mut listen_addresses: HashSet<String> = HashSet::new();
    let mut discovered_peers: HashMap<PeerId, HashSet<libp2p::Multiaddr>> = HashMap::new();

    let mut queue = ClipboardQueue::default();

    loop {
        tokio::select! {
            Some(cmd) = command_rx.recv() => match cmd {
                NetworkCommand::BroadcastText(text) => {
                    if discovered_peers.is_empty() {
                        emit_event(
                            &event_tx,
                            NetworkEvent::NetworkError {
                                peer: "broadcast".to_string(),
                                error: "no peers discovered on the LAN".to_string(),
                            },
                        );
                    } else {
                        emit_event(&event_tx, NetworkEvent::MessageSent { text: text.clone() });
                        let req = ClipboardRequest { text: text.clone() };

                        for peer_id in discovered_peers.keys() {
                            swarm.behaviour_mut().req_res.send_request(&peer_id, req.clone());
                        }
                    }
                }
                NetworkCommand::SendTextTo { target_peer_id, text } => {
                    if let Ok(peer) = PeerId::from_str(&target_peer_id) {
                        let req = ClipboardRequest { text: text.clone() };
                        emit_event(&event_tx, NetworkEvent::MessageSent { text });
                        swarm.behaviour_mut().req_res.send_request(&peer, req);
                    } else {
                        emit_event(
                            &event_tx,
                            NetworkEvent::NetworkError {
                                peer: target_peer_id,
                                error: "invalid peer id".to_string(),
                            },
                        );
                    }
                }
                NetworkCommand::DismissCurrent => {
                    if let Some(dismissed) = queue.next() {
                        let next = queue.peek_current().cloned();
                        emit_event(&event_tx, NetworkEvent::QueueItemDismissed {
                            dismissed,
                            next,
                        });
                    } else {
                        emit_event(&event_tx, NetworkEvent::QueueEmpty);
                    }
                }
                NetworkCommand::StarCurrent => {
                    if let Some(item_to_star) = queue.peek_current().cloned() {
                        let (resp_tx, resp_rx) = oneshot::channel();

                        let send_result = db_tx.send(DbCommand::Insert {
                            item: item_to_star,
                            responder: resp_tx,
                        }).await;

                        if send_result.is_err() {
                            emit_event(
                                &event_tx,
                                NetworkEvent::DatabaseError("Failed to send insert command to DB".to_string()),
                            );
                            continue;
                        }

                        match resp_rx.await {
                            Ok(Ok(())) => {
                                let starred = queue.star_current().unwrap();
                                let next = queue.peek_current().cloned();
                                emit_event(
                                    &event_tx,
                                    NetworkEvent::QueueItemStarred { starred, next }
                                );
                            }
                            Ok(Err(err)) => emit_event(&event_tx, NetworkEvent::DatabaseError(err)),
                            Err(_) => emit_event(
                                &event_tx,
                                NetworkEvent::DatabaseError("DB thread dropped request".to_string())
                            ),
                        }
                    } else {
                        emit_event(&event_tx, NetworkEvent::QueueEmpty);
                    }
                }
                NetworkCommand::ListPending => {
                    let items = queue.list_all().cloned().collect();
                    emit_event(&event_tx, NetworkEvent::QueuePendingList { items });
                }
                NetworkCommand::LoadStarredPage { limit, offset } => {
                    let (resp_tx, resp_rx) = oneshot::channel();
                    let _ = db_tx.send(DbCommand::GetPreviewPage { limit, offset, responder: resp_tx }).await;

                    forward_db_response(resp_rx, event_tx.clone(), move |items| {
                        NetworkEvent::StarredPageLoaded { items, offset }
                    });
                }
                NetworkCommand::Unstar(id) => {
                    let (resp_tx, resp_rx) = oneshot::channel();
                    let _ = db_tx.send(DbCommand::Delete { id, responder: resp_tx }).await;

                    forward_db_response(resp_rx, event_tx.clone(), move |_| {
                        NetworkEvent::QueueItemUnstarred { id }
                    });
                }
                NetworkCommand::GetFullStarredText(id) => {
                    let (resp_tx, resp_rx) = oneshot::channel();
                    let _ = db_tx.send(DbCommand::GetFullText { id, responder: resp_tx }).await;

                    forward_db_response(resp_rx, event_tx.clone(), move |text| {
                        NetworkEvent::StarredTextLoaded { id, text }
                    });
                }
                NetworkCommand::List => {
                    let listen_addresses_list = listen_addresses.iter().cloned().collect();
                    let connected_peers_list = swarm
                        .connected_peers()
                        .map(|peer| peer.to_string())
                        .collect();
                    let discovered_peers_map = discovered_peers
                        .iter()
                        .map(|(peer_id, addrs)| {
                            (
                                peer_id.to_string(),
                                addrs.iter().map(|a| a.to_string()).collect()
                            )
                        })
                        .collect();

                    emit_event(
                        &event_tx,
                        NetworkEvent::List {
                            listen_addresses: listen_addresses_list,
                            discovered_peers: discovered_peers_map,
                            connected_peers: connected_peers_list,
                        },
                    );
                }
            },

            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    let addr_str = address.to_string();
                    if listen_addresses.insert(addr_str.clone()) {
                        emit_event(&event_tx, NetworkEvent::ListeningOn(addr_str));
                    }
                }

                SwarmEvent::ExpiredListenAddr { .. } => {}

                SwarmEvent::Behaviour(BoredBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, addr) in list {
                        swarm.add_peer_address(peer_id, addr.clone());

                        let addrs = discovered_peers.entry(peer_id).or_default();

                        if addrs.is_empty() {
                            emit_event(&event_tx, NetworkEvent::PeerDiscovered(peer_id.to_string()));
                        }

                        addrs.insert(addr);
                    }
                }

                SwarmEvent::Behaviour(BoredBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, addr) in list {
                        if let Some(addrs) = discovered_peers.get_mut(&peer_id) {
                            addrs.remove(&addr);

                            if addrs.is_empty() {
                                discovered_peers.remove(&peer_id);
                                emit_event(&event_tx, NetworkEvent::PeerExpired(peer_id.to_string()));
                            }
                        }
                    }
                }

                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    if swarm.is_connected(&peer_id) {
                        emit_event(&event_tx, NetworkEvent::PeerConnected(peer_id.to_string()));
                    }
                }

                SwarmEvent::ConnectionClosed { peer_id, num_established, .. } => {
                    if num_established == 0 {
                        if !swarm.is_connected(&peer_id) {
                            emit_event(&event_tx, NetworkEvent::PeerDisconnected(peer_id.to_string()));
                        }
                    }
                }

                SwarmEvent::Behaviour(BoredBehaviourEvent::ReqRes(request_response::Event::Message { peer, message, .. })) => {
                    match message {
                        request_response::Message::Request { request, channel, .. } => {
                            let from = peer.to_string();
                            let text = request.text.clone();
                            let queue_was_empty = queue.pending.is_empty();

                            match queue.push_with_source(text.clone(), Some(from.clone())) {
                                Ok(()) => {
                                    let queue_len = queue.pending.len();
                                    let queue_bytes = queue.current_bytes;
                                    emit_event(
                                        &event_tx,
                                        NetworkEvent::MessageReceived {
                                            from,
                                            text,
                                            queue_was_empty,
                                            queue_len,
                                            queue_bytes,
                                        },
                                    );
                                    let _ = swarm.behaviour_mut().req_res.send_response(
                                        channel,
                                        ClipboardResponse {
                                            ack: true,
                                            reason: None,
                                        },
                                    );
                                }
                                Err(QueueError::QueueFull) => {
                                    emit_event(
                                        &event_tx,
                                        NetworkEvent::QueueRejected {
                                            reason: "queue full".to_string(),
                                        },
                                    );
                                    let _ = swarm.behaviour_mut().req_res.send_response(
                                        channel,
                                        ClipboardResponse {
                                            ack: false,
                                            reason: Some("queue full".to_string()),
                                        },
                                    );
                                }
                            }
                        }
                        request_response::Message::Response { response, .. } => {
                            if !response.ack {
                                emit_event(
                                    &event_tx,
                                    NetworkEvent::DeliveryFailed {
                                        peer: peer.to_string(),
                                        reason: response
                                            .reason
                                            .unwrap_or_else(|| "delivery failed".to_string()),
                                    },
                                );
                            }
                        }
                    }
                }

                SwarmEvent::Behaviour(BoredBehaviourEvent::ReqRes(request_response::Event::OutboundFailure {
                    peer,
                    ..
                })) => {
                    emit_event(&event_tx, NetworkEvent::PeerUnreachable(peer.to_string()));
                }

                SwarmEvent::Behaviour(BoredBehaviourEvent::ReqRes(request_response::Event::InboundFailure {
                    peer,
                    error,
                    ..
                })) => {
                    emit_event(
                        &event_tx,
                        NetworkEvent::NetworkError {
                            peer: peer.to_string(),
                            error: format!("Inbound error: {error}"),
                        },
                    );
                }

                SwarmEvent::Behaviour(BoredBehaviourEvent::Ping(_)) => {}

                _ => {}
            }
        }
    }
}

fn forward_db_response<T, E, F>(
    resp_rx: oneshot::Receiver<Result<T, E>>,
    event_tx: mpsc::Sender<NetworkEvent>,
    map_success: F,
) where
    T: Send + 'static,
    E: Display + Send + 'static,
    F: FnOnce(T) -> NetworkEvent + Send + 'static,
{
    tokio::spawn(async move {
        let _ = match resp_rx.await {
            Ok(Ok(val)) => event_tx.send(map_success(val)).await,
            Ok(Err(err)) => {
                event_tx
                    .send(NetworkEvent::DatabaseError(err.to_string()))
                    .await
            }
            Err(_) => {
                event_tx
                    .send(NetworkEvent::DatabaseError(
                        "DB thread dropped request".into(),
                    ))
                    .await
            }
        };
    });
}

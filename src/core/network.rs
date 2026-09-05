use super::protocol::{ClipboardRequest, ClipboardResponse, NetworkCommand, NetworkEvent};
use futures::StreamExt;
use libp2p::{
    PeerId, StreamProtocol, identity, mdns,
    request_response::{self, ProtocolSupport},
    swarm::{NetworkBehaviour, SwarmEvent},
};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    str::FromStr,
    time::Duration,
};
use tokio::sync::mpsc;

#[derive(NetworkBehaviour)]
pub struct BoredBehaviour {
    pub req_res: request_response::cbor::Behaviour<ClipboardRequest, ClipboardResponse>,
    pub mdns: mdns::tokio::Behaviour,
}

fn emit_event(event_tx: &mpsc::Sender<NetworkEvent>, event: NetworkEvent) {
    let _ = event_tx.try_send(event);
}

pub async fn run_network_loop(
    mut command_rx: mpsc::Receiver<NetworkCommand>,
    event_tx: mpsc::Sender<NetworkEvent>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let local_key = identity::Keypair::generate_ed25519();

    // Build the libp2p swarm for LAN discovery and clipboard request/response messages.
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
            Ok(BoredBehaviour { req_res, mdns })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(30)))
        .build();

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    // Track peers discovered on the LAN by active multiaddr set, so a peer is not dropped
    // until every discovered route has actually expired.
    let mut discovered_peers: HashMap<PeerId, HashSet<libp2p::Multiaddr>> = HashMap::new();
    // Track actual TCP connections so the UI reflects real online/offline state.
    let mut connected_peers: HashSet<PeerId> = HashSet::new();
    // Track listen addresses to avoid duplicate "ListeningOn" events.
    let mut listen_addresses: HashSet<String> = HashSet::new();

    loop {
        tokio::select! {
            // Handle outbound commands coming from the app or CLI.
            Some(cmd) = command_rx.recv() => match cmd {
                NetworkCommand::BroadcastText(text) => {
                    let req = ClipboardRequest { text: text.clone() };

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
                        for peer in discovered_peers.keys() {
                            swarm.behaviour_mut().req_res.send_request(peer, req.clone());
                        }
                    }
                }
                NetworkCommand::SendTextTo { target_peer_id, text } => {
                    if let Ok(peer) = PeerId::from_str(&target_peer_id) {
                        let req = ClipboardRequest { text: text.clone() };
                        emit_event(&event_tx, NetworkEvent::MessageSent { text: text.clone() });
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
            },

            // React to discovery and message events emitted by libp2p.
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    let addr_str = address.to_string();
                    if listen_addresses.insert(addr_str.clone()) {
                        emit_event(&event_tx, NetworkEvent::ListeningOn(addr_str));
                    }
                }

                SwarmEvent::ExpiredListenAddr { address, .. } => {
                    let addr_str = address.to_string();
                    listen_addresses.remove(&addr_str);
                }

                SwarmEvent::Behaviour(BoredBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, addr) in list {
                        swarm.add_peer_address(peer_id, addr.clone());

                        let addresses = discovered_peers.entry(peer_id).or_default();
                        if addresses.is_empty() {
                            emit_event(&event_tx, NetworkEvent::PeerDiscovered(peer_id.to_string()));
                        }
                        addresses.insert(addr);
                    }
                }

                SwarmEvent::Behaviour(BoredBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, addr) in list {
                        if let Some(addresses) = discovered_peers.get_mut(&peer_id) {
                            addresses.remove(&addr);
                            if addresses.is_empty() {
                                discovered_peers.remove(&peer_id);
                                emit_event(&event_tx, NetworkEvent::PeerExpired(peer_id.to_string()));
                            }
                        }
                    }
                }

                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    if connected_peers.insert(peer_id) {
                        emit_event(&event_tx, NetworkEvent::PeerConnected(peer_id.to_string()));
                    }
                }

                SwarmEvent::ConnectionClosed { peer_id, .. } => {
                    if connected_peers.remove(&peer_id) {
                        emit_event(&event_tx, NetworkEvent::PeerDisconnected(peer_id.to_string()));
                    }
                }

                SwarmEvent::Behaviour(BoredBehaviourEvent::ReqRes(request_response::Event::Message { peer, message, .. })) => {
                    match message {
                        request_response::Message::Request { request, channel, .. } => {
                            emit_event(
                                &event_tx,
                                NetworkEvent::MessageReceived {
                                    from: peer.to_string(),
                                    text: request.text,
                                },
                            );

                            let _ = swarm.behaviour_mut().req_res.send_response(
                                channel,
                                ClipboardResponse { ack: true },
                            );
                        }
                        request_response::Message::Response { .. } => {
                            // The peer acknowledged receipt of a message sent earlier.
                        }
                    }
                }

                SwarmEvent::Behaviour(BoredBehaviourEvent::ReqRes(request_response::Event::OutboundFailure {
                    peer,
                    error,
                    ..
                })) => {
                    emit_event(
                        &event_tx,
                        NetworkEvent::NetworkError {
                            peer: peer.to_string(),
                            error: error.to_string(),
                        },
                    );
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
                            error: error.to_string(),
                        },
                    );
                }

                _ => {}
            }
        }
    }
}

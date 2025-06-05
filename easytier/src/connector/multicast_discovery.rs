use std::{
    collections::{HashMap, HashSet},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{
    net::UdpSocket,
    sync::{broadcast, mpsc},
    time::{sleep, timeout, Instant},
};
use url::Url;

use crate::{
    common::{
        error::Error,
        global_ctx::ArcGlobalCtx,
        network::IPCollector,
        PeerId,
    },
    connector::create_connector_by_url,
    tunnel::{TunnelConnector, IpVersion},
};

const MULTICAST_ADDR: &str = "224.0.0.251";
const MULTICAST_PORT: u16 = 55301; // Changed from 5353 to avoid mDNS conflicts
const DISCOVERY_INTERVAL: Duration = Duration::from_secs(10); // Faster discovery - every 10 seconds
const QUERY_TIMEOUT: Duration = Duration::from_secs(2); // Faster initial timeout
const PEER_TTL: Duration = Duration::from_secs(90);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiscoveryMessage {
    message_type: MessageType,
    peer_id: PeerId,
    network_name: String,
    network_secret_digest: Option<[u8; 32]>,
    listeners: Vec<String>,
    hostname: String,
    instance_id: String,
    version: String,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum MessageType {
    Announcement,
    Query,
    Response,
}

#[derive(Debug, Clone)]
pub struct DiscoveredPeer {
    pub peer_id: PeerId,
    pub listeners: Vec<Url>,
    pub hostname: String,
    pub instance_id: String,
    pub last_seen: Instant,
}

pub struct MulticastDiscoveryConnector {
    global_ctx: ArcGlobalCtx,
    peer_manager: Option<std::sync::Weak<crate::peers::peer_manager::PeerManager>>,
    discovered_peers: Arc<Mutex<HashMap<PeerId, DiscoveredPeer>>>,
    socket: Option<Arc<UdpSocket>>,
    shutdown_tx: Option<broadcast::Sender<()>>,
    peer_tx: Option<mpsc::UnboundedSender<DiscoveredPeer>>,
}

impl std::fmt::Debug for MulticastDiscoveryConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MulticastDiscoveryConnector")
            .field("network", &self.global_ctx.get_network_name())
            .finish()
    }
}

impl MulticastDiscoveryConnector {
    pub fn new(global_ctx: ArcGlobalCtx) -> Self {
        Self {
            global_ctx,
            peer_manager: None,
            discovered_peers: Arc::new(Mutex::new(HashMap::new())),
            socket: None,
            shutdown_tx: None,
            peer_tx: None,
        }
    }

    pub fn new_with_peer_manager(
        global_ctx: ArcGlobalCtx, 
        peer_manager: std::sync::Arc<crate::peers::peer_manager::PeerManager>
    ) -> Self {
        Self {
            global_ctx,
            peer_manager: Some(std::sync::Arc::downgrade(&peer_manager)),
            discovered_peers: Arc::new(Mutex::new(HashMap::new())),
            socket: None,
            shutdown_tx: None,
            peer_tx: None,
        }
    }

    async fn create_multicast_socket(&self) -> Result<Arc<UdpSocket>, Error> {
        let bind_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, MULTICAST_PORT);
        tracing::info!(?bind_addr, "Attempting to bind multicast socket");
        
        // Create socket with SO_REUSEADDR and SO_REUSEPORT to allow multiple instances on same machine
        let socket2 = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))
            .with_context(|| "Failed to create socket")?;
        
        socket2.set_reuse_address(true)
            .with_context(|| "Failed to set SO_REUSEADDR")?;
        
        // SO_REUSEPORT is platform-specific, so we'll try to set it but continue if it fails
        if let Err(e) = socket2.set_reuse_port(true) {
            tracing::warn!(?e, "Failed to set SO_REUSEPORT, continuing anyway");
        }
        
        socket2.bind(&bind_addr.into())
            .with_context(|| format!("Failed to bind multicast socket to {}", bind_addr))?;
        
        socket2.set_nonblocking(true)
            .with_context(|| "Failed to set non-blocking")?;
        
        let socket = UdpSocket::from_std(socket2.into())
            .with_context(|| "Failed to convert to tokio socket")?;

        tracing::info!("Successfully bound multicast socket, setting broadcast");
        socket.set_broadcast(true)
            .with_context(|| "Failed to set broadcast")?;

        // Join multicast group on all available interfaces
        let multicast_addr: Ipv4Addr = MULTICAST_ADDR.parse().unwrap();
        let ip_collector = self.global_ctx.get_ip_collector();
        let ips = ip_collector.collect_ip_addrs().await;

        tracing::info!(
            ?multicast_addr,
            interface_count = ips.interface_ipv4s.len(),
            "Joining multicast group on interfaces"
        );

        let mut join_success_count = 0;
        for interface_ip in &ips.interface_ipv4s {
            if let Err(e) = socket.join_multicast_v4(multicast_addr, (*interface_ip).into()) {
                tracing::warn!(?e, ?interface_ip, "Failed to join multicast group on interface");
            } else {
                tracing::info!(?interface_ip, "Successfully joined multicast group on interface");
                join_success_count += 1;
            }
        }

        if join_success_count == 0 {
            tracing::warn!("Failed to join multicast group on any interface, trying default");
            // Try joining on default interface (0.0.0.0)
            if let Err(e) = socket.join_multicast_v4(multicast_addr, Ipv4Addr::UNSPECIFIED) {
                tracing::error!(?e, "Failed to join multicast group on default interface");
            } else {
                tracing::info!("Successfully joined multicast group on default interface");
                join_success_count += 1;
            }
        }

        tracing::info!(?join_success_count, "Multicast socket setup complete");
        Ok(Arc::new(socket))
    }

    fn create_discovery_message(&self, message_type: MessageType) -> DiscoveryMessage {
        let network_identity = self.global_ctx.get_network_identity();
        let bind_listeners = self.global_ctx.get_running_listeners();
        
        // Get the actual peer ID from peer manager if available
        let peer_id = if let Some(peer_mgr_weak) = &self.peer_manager {
            if let Some(peer_mgr) = peer_mgr_weak.upgrade() {
                peer_mgr.my_peer_id()
            } else {
                tracing::warn!("Peer manager reference is stale, using random peer ID");
                rand::random()
            }
        } else {
            tracing::debug!("No peer manager available, using random peer ID");
            rand::random()
        };
        
        // Convert wildcard listeners to connectable addresses
        let connectable_listeners = self.create_connectable_listeners(&bind_listeners);
        
        tracing::debug!(
            ?peer_id,
            ?message_type,
            bind_listeners_count = bind_listeners.len(),
            connectable_listeners_count = connectable_listeners.len(),
            network_name = %network_identity.network_name,
            connectable_listeners = ?connectable_listeners,
            "Creating discovery message"
        );
        
        DiscoveryMessage {
            message_type,
            peer_id,
            network_name: network_identity.network_name,
            network_secret_digest: network_identity.network_secret_digest,
            listeners: connectable_listeners,
            hostname: self.global_ctx.get_hostname(),
            instance_id: self.global_ctx.get_id().to_string(),
            version: crate::common::constants::EASYTIER_VERSION.to_string(),
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    fn create_connectable_listeners(&self, bind_listeners: &[Url]) -> Vec<String> {
        let ip_collector = self.global_ctx.get_ip_collector();
        let ips = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(ip_collector.collect_ip_addrs())
        });
        
        let mut connectable_listeners = Vec::new();
        
        for bind_listener in bind_listeners {
            // Skip ring:// listeners as they use UUIDs, not IP addresses
            if bind_listener.scheme() == "ring" {
                connectable_listeners.push(bind_listener.to_string());
                continue;
            }
            
            if let Some(host_str) = bind_listener.host_str() {
                let port = bind_listener.port();
                
                if host_str == "0.0.0.0" {
                    // Replace with all IPv4 interface addresses
                    for ipv4 in &ips.interface_ipv4s {
                        let ipv4_str = ipv4.to_string();
                        let new_url = if bind_listener.path().is_empty() || bind_listener.path() == "/" {
                            format!("{}://{}:{}", 
                                bind_listener.scheme(),
                                ipv4_str,
                                port.unwrap_or(80)
                            )
                        } else {
                            format!("{}://{}:{}{}", 
                                bind_listener.scheme(),
                                ipv4_str,
                                port.unwrap_or(80),
                                bind_listener.path()
                            )
                        };
                        connectable_listeners.push(new_url);
                    }
                } else if host_str == "::" {
                    // Replace with all IPv6 interface addresses
                    for ipv6 in &ips.interface_ipv6s {
                        let ipv6_str = ipv6.to_string();
                        let new_url = if bind_listener.path().is_empty() || bind_listener.path() == "/" {
                            format!("{}://[{}]:{}", 
                                bind_listener.scheme(),
                                ipv6_str,
                                port.unwrap_or(80)
                            )
                        } else {
                            format!("{}://[{}]:{}{}", 
                                bind_listener.scheme(),
                                ipv6_str,
                                port.unwrap_or(80),
                                bind_listener.path()
                            )
                        };
                        connectable_listeners.push(new_url);
                    }
                } else {
                    // Keep as-is for specific addresses
                    connectable_listeners.push(bind_listener.to_string());
                }
            } else {
                // Keep as-is for non-IP listeners
                connectable_listeners.push(bind_listener.to_string());
            }
        }
        
        tracing::debug!(
            original_count = bind_listeners.len(),
            connectable_count = connectable_listeners.len(),
            "Converted bind listeners to connectable listeners"
        );
        
        connectable_listeners
    }

    async fn send_discovery_message(&self, message: &DiscoveryMessage) -> Result<(), Error> {
        let socket = self.socket.as_ref().ok_or(Error::Unknown)?;
        let multicast_addr = SocketAddrV4::new(MULTICAST_ADDR.parse().unwrap(), MULTICAST_PORT);
        
        let message_bytes = bincode::serialize(message)
            .with_context(|| "Failed to serialize discovery message")?;
        
        tracing::debug!(
            message_type = ?message.message_type,
            peer_id = message.peer_id,
            message_size = message_bytes.len(),
            target_addr = %multicast_addr,
            listeners_count = message.listeners.len(),
            "Sending multicast discovery message"
        );
        
        let bytes_sent = socket.send_to(&message_bytes, multicast_addr).await
            .with_context(|| format!("Failed to send multicast message to {}", multicast_addr))?;
        
        tracing::debug!(?bytes_sent, "Successfully sent multicast message");
        Ok(())
    }

    async fn handle_received_message(&self, message: DiscoveryMessage, from: SocketAddr) -> Result<(), Error> {
        tracing::debug!(
            message_type = ?message.message_type,
            peer_id = message.peer_id,
            network_name = %message.network_name,
            hostname = %message.hostname,
            listeners_count = message.listeners.len(),
            from_addr = %from,
            "Received multicast discovery message"
        );

        // Ignore our own messages
        if message.instance_id == self.global_ctx.get_id().to_string() {
            tracing::debug!("Ignoring message from self");
            return Ok(());
        }

        // Check network compatibility
        let our_identity = self.global_ctx.get_network_identity();
        if message.network_name != our_identity.network_name {
            tracing::debug!(
                their_network = %message.network_name,
                our_network = %our_identity.network_name,
                "Network name mismatch, ignoring message"
            );
            return Ok(());
        }

        // Verify network secret if both sides have it
        if let (Some(our_digest), Some(their_digest)) = 
            (our_identity.network_secret_digest, message.network_secret_digest) {
            if our_digest != their_digest {
                tracing::warn!(
                    peer_id = message.peer_id,
                    "Network secret mismatch from peer, ignoring message"
                );
                return Ok(());
            }
        }

        // Parse listener URLs
        let mut listeners = Vec::new();
        for listener_str in &message.listeners {
            if let Ok(url) = listener_str.parse::<Url>() {
                listeners.push(url);
            } else {
                tracing::warn!(
                    invalid_url = %listener_str,
                    peer_id = message.peer_id,
                    "Peer advertised invalid listener URL"
                );
            }
        }

        if listeners.is_empty() {
            tracing::debug!(
                peer_id = message.peer_id,
                hostname = %message.hostname,
                "Peer has no valid listeners, ignoring"
            );
            return Ok(());
        }

        let discovered_peer = DiscoveredPeer {
            peer_id: message.peer_id,
            listeners,
            hostname: message.hostname,
            instance_id: message.instance_id,
            last_seen: Instant::now(),
        };

        // Update discovered peers
        let is_new_peer = {
            let mut peers = self.discovered_peers.lock().unwrap();
            let is_new = !peers.contains_key(&message.peer_id);
            peers.insert(message.peer_id, discovered_peer.clone());
            is_new
        };

        if is_new_peer {
            tracing::info!(
                peer_id = message.peer_id,
                hostname = %discovered_peer.hostname,
                listeners = ?discovered_peer.listeners,
                "New peer discovered via multicast!"
            );
        } else {
            tracing::debug!(
                peer_id = message.peer_id,
                "Updated existing discovered peer"
            );
        }

        // Notify about new peer
        if let Some(tx) = &self.peer_tx {
            tracing::debug!("Sending discovered peer to handler channel");
            if let Err(_) = tx.send(discovered_peer) {
                tracing::warn!("Failed to send discovered peer to handler");
            } else {
                tracing::debug!("Successfully sent discovered peer to handler");
            }
        } else {
            tracing::warn!("No peer_tx channel available to send discovered peer");
        }

        // Respond to queries
        if matches!(message.message_type, MessageType::Query) {
            let response = self.create_discovery_message(MessageType::Response);
            if let Err(e) = self.send_discovery_message(&response).await {
                tracing::warn!(?e, "Failed to send discovery response");
            }
        }

        Ok(())
    }

    async fn discovery_loop(&self, mut shutdown_rx: broadcast::Receiver<()>) -> Result<(), Error> {
        let socket = self.socket.as_ref().ok_or(Error::Unknown)?;
        let mut interval = tokio::time::interval(DISCOVERY_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Send initial query
        tracing::info!("Starting multicast discovery loop, sending initial query");
        let query = self.create_discovery_message(MessageType::Query);
        if let Err(e) = self.send_discovery_message(&query).await {
            tracing::warn!(?e, "Failed to send initial discovery query");
        } else {
            tracing::info!("Successfully sent initial discovery query");
        }

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    tracing::info!("Multicast discovery loop shutting down");
                    break;
                }
                _ = interval.tick() => {
                    // Send periodic announcement
                    tracing::debug!("Sending periodic discovery announcement");
                    let announcement = self.create_discovery_message(MessageType::Announcement);
                    if let Err(e) = self.send_discovery_message(&announcement).await {
                        tracing::warn!(?e, "Failed to send discovery announcement");
                    }

                    // Clean up stale peers
                    let peer_count_before = self.discovered_peers.lock().unwrap().len();
                    self.cleanup_stale_peers().await;
                    let peer_count_after = self.discovered_peers.lock().unwrap().len();
                    
                    if peer_count_before != peer_count_after {
                        tracing::debug!(
                            peers_before = peer_count_before,
                            peers_after = peer_count_after,
                            "Cleaned up stale discovered peers"
                        );
                    }
                }
                result = self.receive_message(socket) => {
                    if let Err(e) = result {
                        tracing::warn!(?e, "Error receiving multicast message");
                    }
                }
            }
        }

        Ok(())
    }

    async fn receive_message(&self, socket: &UdpSocket) -> Result<(), Error> {
        let mut buf = [0u8; 8192];
        let (len, from) = socket.recv_from(&mut buf).await
            .with_context(|| "Failed to receive multicast message")?;

        let message: DiscoveryMessage = bincode::deserialize(&buf[..len])
            .with_context(|| "Failed to deserialize discovery message")?;

        self.handle_received_message(message, from).await
    }

    async fn cleanup_stale_peers(&self) {
        let mut peers = self.discovered_peers.lock().unwrap();
        let now = Instant::now();
        peers.retain(|_, peer| now.duration_since(peer.last_seen) < PEER_TTL);
    }

    pub async fn start_discovery(&mut self) -> Result<mpsc::UnboundedReceiver<DiscoveredPeer>, Error> {
        if self.socket.is_some() {
            return Err(Error::AnyhowError(anyhow::anyhow!("Discovery already started")));
        }

        let socket = self.create_multicast_socket().await?;
        self.socket = Some(socket);

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        let (peer_tx, peer_rx) = mpsc::unbounded_channel();
        self.peer_tx = Some(peer_tx);

        let connector = self.clone();
        tokio::spawn(async move {
            if let Err(e) = connector.discovery_loop(shutdown_rx).await {
                tracing::error!(?e, "Discovery loop failed");
            }
        });

        Ok(peer_rx)
    }

    pub fn stop_discovery(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.socket = None;
        self.peer_tx = None;
    }

    pub fn list_discovered_peers(&self) -> Vec<DiscoveredPeer> {
        let peers = self.discovered_peers.lock().unwrap();
        peers.values().cloned().collect()
    }
}

impl Clone for MulticastDiscoveryConnector {
    fn clone(&self) -> Self {
        Self {
            global_ctx: self.global_ctx.clone(),
            peer_manager: self.peer_manager.clone(),
            discovered_peers: self.discovered_peers.clone(),
            socket: self.socket.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            peer_tx: self.peer_tx.clone(),
        }
    }
}

#[async_trait]
impl TunnelConnector for MulticastDiscoveryConnector {
    async fn connect(&mut self) -> Result<Box<dyn crate::tunnel::Tunnel>, crate::tunnel::TunnelError> {
        // This connector doesn't create tunnels directly
        // Instead, it discovers peers and connects through their advertised protocols
        
        // Start discovery if not already started
        let peer_rx = if self.socket.is_none() {
            Some(self.start_discovery().await.map_err(|e| crate::tunnel::TunnelError::InvalidPacket(e.to_string()))?)
        } else {
            None
        };

        // Wait for peer discovery with timeout
        let discovered_peer = if let Some(mut rx) = peer_rx {
            timeout(QUERY_TIMEOUT, rx.recv()).await
                .map_err(|e| crate::tunnel::TunnelError::InvalidPacket(e.to_string()))?
                .ok_or(crate::tunnel::TunnelError::InvalidPacket("Discovery channel closed".to_string()))?
        } else {
            // Get the most recently seen peer
            let peers = self.discovered_peers.lock().unwrap();
            let peer = peers.values()
                .max_by_key(|p| p.last_seen)
                .ok_or(crate::tunnel::TunnelError::InvalidPacket("No peers discovered".to_string()))?
                .clone();
            peer
        };

        // Try to connect using the peer's advertised listeners
        for listener_url in &discovered_peer.listeners {
            match create_connector_by_url(
                &listener_url.to_string(),
                &self.global_ctx,
                IpVersion::Both,
            ).await {
                Ok(mut connector) => {
                    match connector.connect().await {
                        Ok(tunnel) => {
                            tracing::info!(
                                ?discovered_peer.peer_id,
                                ?listener_url,
                                "Successfully connected to discovered peer"
                            );
                            return Ok(tunnel);
                        }
                        Err(e) => {
                            tracing::debug!(?e, ?listener_url, "Failed to connect to discovered peer");
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!(?e, ?listener_url, "Failed to create connector for discovered peer");
                }
            }
        }

        Err(crate::tunnel::TunnelError::InvalidPacket(format!(
            "Failed to connect to any listeners of peer {}",
            discovered_peer.peer_id
        )))
    }

    fn remote_url(&self) -> Url {
        format!("multicast://{}:{}", MULTICAST_ADDR, MULTICAST_PORT)
            .parse()
            .unwrap()
    }

    fn set_bind_addrs(&mut self, _addrs: Vec<SocketAddr>) {
        // Multicast discovery uses all available interfaces
    }

    fn set_ip_version(&mut self, _ip_version: IpVersion) {
        // Multicast discovery currently only supports IPv4
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::global_ctx::tests::get_mock_global_ctx;

    #[tokio::test]
    async fn test_multicast_discovery_message_serialization() {
        let message = DiscoveryMessage {
            message_type: MessageType::Announcement,
            peer_id: 12345,
            network_name: "test_network".to_string(),
            network_secret_digest: Some([1u8; 32]),
            listeners: vec!["tcp://192.168.1.100:8080".to_string()],
            hostname: "test_host".to_string(),
            instance_id: "test_instance".to_string(),
            version: "1.0.0".to_string(),
            timestamp: 1234567890,
        };

        let serialized = bincode::serialize(&message).unwrap();
        let deserialized: DiscoveryMessage = bincode::deserialize(&serialized).unwrap();

        assert_eq!(message.peer_id, deserialized.peer_id);
        assert_eq!(message.network_name, deserialized.network_name);
        assert_eq!(message.listeners, deserialized.listeners);
    }

    #[tokio::test]
    async fn test_multicast_connector_creation() {
        let global_ctx = get_mock_global_ctx();
        let connector = MulticastDiscoveryConnector::new(global_ctx);
        
        assert!(connector.socket.is_none());
        assert_eq!(
            connector.remote_url().to_string(),
            "multicast://224.0.0.251:55301"
        );
    }

    #[tokio::test]
    async fn test_connectable_listeners_conversion() {
        let global_ctx = get_mock_global_ctx();
        let connector = MulticastDiscoveryConnector::new(global_ctx);
        
        // Test wildcard address conversion
        let bind_listeners = vec![
            "tcp://0.0.0.0:11010".parse().unwrap(),
            "udp://[::]:11010".parse().unwrap(),
            "tcp://192.168.1.100:11010".parse().unwrap(),
            "ring://some-uuid".parse().unwrap(),
        ];
        
        let connectable = connector.create_connectable_listeners(&bind_listeners);
        
        // Should have more connectable listeners than bind listeners due to wildcard expansion
        assert!(connectable.len() >= bind_listeners.len());
        
        // Should not contain wildcard addresses
        assert!(!connectable.iter().any(|url| url.contains("0.0.0.0")));
        assert!(!connectable.iter().any(|url| url.contains("[::]")));
        
        // Should contain the specific address unchanged
        assert!(connectable.iter().any(|url| url.contains("192.168.1.100:11010")));
        
        // Should contain the ring URL unchanged
        assert!(connectable.iter().any(|url| url.starts_with("ring://")));
    }
}
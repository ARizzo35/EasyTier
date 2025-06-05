use std::sync::Arc;

use tokio::{sync::mpsc, task::JoinSet};

use crate::{
    common::{error::Error, global_ctx::ArcGlobalCtx},
    peers::peer_manager::PeerManager,
};

use super::{multicast_discovery::MulticastDiscoveryConnector, manual::ManualConnectorManager};

pub struct DiscoveryManager {
    global_ctx: ArcGlobalCtx,
    peer_manager: Arc<PeerManager>,
    manual_connector_manager: Arc<ManualConnectorManager>,
    multicast_connector: Option<Arc<tokio::sync::Mutex<MulticastDiscoveryConnector>>>,
    tasks: JoinSet<()>,
}

impl DiscoveryManager {
    pub fn new(
        global_ctx: ArcGlobalCtx,
        peer_manager: Arc<PeerManager>,
        manual_connector_manager: Arc<ManualConnectorManager>,
    ) -> Self {
        Self {
            global_ctx,
            peer_manager,
            manual_connector_manager,
            multicast_connector: None,
            tasks: JoinSet::new(),
        }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        tracing::info!("Starting discovery manager");

        // Start multicast discovery if enabled
        if self.global_ctx.config.get_flags().enable_multicast_discovery {
            self.start_multicast_discovery().await?;
        }

        Ok(())
    }

    async fn start_multicast_discovery(&mut self) -> Result<(), Error> {
        tracing::info!("Starting multicast peer discovery");

        let mut multicast_connector = MulticastDiscoveryConnector::new_with_peer_manager(
            self.global_ctx.clone(),
            self.peer_manager.clone()
        );
        let peer_rx = multicast_connector.start_discovery().await?;

        let connector_arc = Arc::new(tokio::sync::Mutex::new(multicast_connector));
        self.multicast_connector = Some(connector_arc.clone());

        // Spawn task to handle discovered peers
        let manual_mgr = self.manual_connector_manager.clone();
        let global_ctx = self.global_ctx.clone();
        let peer_mgr = self.peer_manager.clone();

        self.tasks.spawn(async move {
            Self::handle_discovered_peers(peer_rx, manual_mgr, global_ctx, peer_mgr).await;
        });

        Ok(())
    }

    async fn handle_discovered_peers(
        mut peer_rx: mpsc::UnboundedReceiver<super::multicast_discovery::DiscoveredPeer>,
        manual_mgr: Arc<ManualConnectorManager>,
        global_ctx: ArcGlobalCtx,
        peer_mgr: Arc<PeerManager>,
    ) {
        tracing::info!("Starting multicast peer discovery handler");
        
        while let Some(discovered_peer) = peer_rx.recv().await {
            tracing::info!(
                peer_id = discovered_peer.peer_id,
                hostname = %discovered_peer.hostname,
                listeners_count = discovered_peer.listeners.len(),
                listeners = ?discovered_peer.listeners,
                "Discovered new peer via multicast"
            );

            // Try to connect to the discovered peer using the manual connector manager
            for listener_url in &discovered_peer.listeners {
                // Skip if we already have a connector for this URL
                let url_string = listener_url.to_string();
                if manual_mgr
                    .list_connectors()
                    .await
                    .iter()
                    .any(|c| c.url.as_ref().map(|u| u.to_string()) == Some(url_string.clone()))
                {
                    tracing::debug!(?listener_url, "Already have connector for discovered peer");
                    continue;
                }

                // Check if this would connect to ourselves
                let is_self = Self::is_our_listener(&url_string, &global_ctx);
                if is_self {
                    tracing::debug!(
                        ?listener_url, 
                        our_listeners = ?global_ctx.get_running_listeners(),
                        "Skipping self-connection"
                    );
                    continue;
                }

                // Add the discovered peer as a connector
                tracing::debug!(
                    peer_id = discovered_peer.peer_id,
                    ?listener_url,
                    "Attempting to add discovered peer as connector"
                );
                
                match manual_mgr.add_connector_by_url(&url_string).await {
                    Ok(()) => {
                        tracing::info!(
                            peer_id = discovered_peer.peer_id,
                            ?listener_url,
                            "Successfully added discovered peer as connector"
                        );
                        break; // Successfully added one listener, no need to try others
                    }
                    Err(e) => {
                        tracing::warn!(
                            ?e, 
                            peer_id = discovered_peer.peer_id,
                            ?listener_url, 
                            "Failed to add discovered peer as connector"
                        );
                    }
                }
            }

            // Also try to connect directly for immediate connection
            for listener_url in &discovered_peer.listeners {
                let url_string = listener_url.to_string();
                
                let is_self_direct = Self::is_our_listener(&url_string, &global_ctx);
                if is_self_direct {
                    tracing::debug!(
                        ?listener_url,
                        "Skipping direct connection to self"
                    );
                    continue;
                }

                tracing::debug!(
                    peer_id = discovered_peer.peer_id,
                    ?listener_url,
                    "Attempting direct connection to discovered peer"
                );
                
                match super::create_connector_by_url(
                    &url_string,
                    &global_ctx,
                    crate::tunnel::IpVersion::Both,
                ).await {
                    Ok(connector) => {
                        let peer_mgr_clone = peer_mgr.clone();
                        let discovered_peer_id = discovered_peer.peer_id;
                        let url_for_log = url_string.clone();
                        
                        tokio::spawn(async move {
                            tracing::debug!(
                                peer_id = discovered_peer_id,
                                url = %url_for_log,
                                "Starting direct connection attempt"
                            );
                            
                            match peer_mgr_clone.try_direct_connect(connector).await {
                                Ok((peer_id, conn_id)) => {
                                    tracing::info!(
                                        ?peer_id,
                                        ?conn_id,
                                        url = %url_for_log,
                                        "Successfully connected to discovered peer via direct connection"
                                    );
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        ?e, 
                                        peer_id = discovered_peer_id,
                                        url = %url_for_log, 
                                        "Failed to connect to discovered peer via direct connection"
                                    );
                                }
                            }
                        });
                        break; // Try only one listener for immediate connection
                    }
                    Err(e) => {
                        tracing::warn!(
                            ?e, 
                            peer_id = discovered_peer.peer_id,
                            ?listener_url, 
                            "Failed to create connector for discovered peer"
                        );
                    }
                }
            }
        }
        tracing::warn!("Multicast discovery peer receiver closed");
    }

    fn is_our_listener(url_string: &str, global_ctx: &ArcGlobalCtx) -> bool {
        let our_listeners = global_ctx.get_running_listeners();
        let is_self = our_listeners.iter().any(|our_url| {
            let exact_match = our_url.to_string() == url_string;
            let same_endpoint = Self::same_host_port(our_url, url_string);
            
            tracing::trace!(
                our_url = %our_url,
                their_url = %url_string,
                exact_match = exact_match,
                same_endpoint = same_endpoint,
                "Checking if URL is our listener"
            );
            
            exact_match || same_endpoint
        });
        
        tracing::debug!(
            their_url = %url_string,
            our_listeners = ?our_listeners,
            is_self = is_self,
            "Listener self-check result"
        );
        
        is_self
    }

    fn same_host_port(our_url: &url::Url, other_url: &str) -> bool {
        if let Ok(other_parsed) = url::Url::parse(other_url) {
            let ports_match = our_url.port() == other_parsed.port();
            
            // Check for exact host match first
            if our_url.host() == other_parsed.host() && ports_match {
                return true;
            }
            
            // Check if our listener is 0.0.0.0 (bind all) and ports match
            // This is a common case where we bind to 0.0.0.0 but advertise specific IPs
            if let (Some(our_host), Some(their_host)) = (our_url.host_str(), other_parsed.host_str()) {
                if (our_host == "0.0.0.0" || our_host == "[::]") && ports_match {
                    // Our listener binds to all interfaces, so any specific IP on same port
                    // could potentially be our own listener advertised with a specific IP
                    // But we need to be more careful here - only match if it's actually us
                    tracing::trace!(
                        our_host = our_host,
                        their_host = their_host,
                        our_port = ?our_url.port(),
                        their_port = ?other_parsed.port(),
                        "Checking wildcard listener against specific host"
                    );
                    // For now, let's be conservative and not match wildcard binds
                    return false;
                }
            }
            
            false
        } else {
            false
        }
    }

    pub async fn stop(&mut self) {
        tracing::info!("Stopping discovery manager");

        // Stop multicast discovery
        if let Some(connector) = &self.multicast_connector {
            connector.lock().await.stop_discovery();
        }

        // Abort all tasks
        self.tasks.abort_all();
        while let Some(_) = self.tasks.join_next().await {}

        self.multicast_connector = None;
        tracing::info!("Discovery manager stopped");
    }

    pub async fn list_discovered_multicast_peers(&self) -> Vec<super::multicast_discovery::DiscoveredPeer> {
        if let Some(connector) = &self.multicast_connector {
            connector.lock().await.list_discovered_peers()
        } else {
            Vec::new()
        }
    }
}

impl Drop for DiscoveryManager {
    fn drop(&mut self) {
        self.tasks.abort_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::global_ctx::tests::get_mock_global_ctx;
    use crate::peers::tests::create_mock_peer_manager;
    use crate::connector::manual::ManualConnectorManager;

    #[tokio::test]
    async fn test_discovery_manager_creation() {
        let global_ctx = get_mock_global_ctx();
        let peer_mgr = create_mock_peer_manager().await;
        let manual_mgr = Arc::new(ManualConnectorManager::new(
            global_ctx.clone(),
            peer_mgr.clone(),
        ));

        let mut discovery_mgr = DiscoveryManager::new(
            global_ctx.clone(),
            peer_mgr.clone(),
            manual_mgr,
        );

        // Test that we can start and stop the manager
        discovery_mgr.start().await.unwrap();
        discovery_mgr.stop().await;
    }

    #[test]
    fn test_is_our_listener() {
        let global_ctx = get_mock_global_ctx();
        
        // Test same URL detection
        assert!(DiscoveryManager::is_our_listener(
            "tcp://127.0.0.1:8080",
            &global_ctx
        ) == false); // No listeners in mock

        // Test same host:port detection
        assert!(DiscoveryManager::same_host_port(
            &"tcp://127.0.0.1:8080".parse().unwrap(),
            "udp://127.0.0.1:8080"
        ));

        assert!(!DiscoveryManager::same_host_port(
            &"tcp://127.0.0.1:8080".parse().unwrap(),
            "tcp://127.0.0.1:8081"
        ));
    }
}
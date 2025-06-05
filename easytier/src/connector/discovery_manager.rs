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
                if Self::is_our_listener(&url_string, &global_ctx) {
                    tracing::debug!(?listener_url, "Skipping self-connection");
                    continue;
                }

                // Add the discovered peer as a connector
                match manual_mgr.add_connector_by_url(&url_string).await {
                    Ok(()) => {
                        tracing::info!(
                            ?discovered_peer.peer_id,
                            ?listener_url,
                            "Added discovered peer as connector"
                        );
                        break; // Successfully added one listener, no need to try others
                    }
                    Err(e) => {
                        tracing::debug!(?e, ?listener_url, "Failed to add discovered peer as connector");
                    }
                }
            }

            // Also try to connect directly for immediate connection
            for listener_url in &discovered_peer.listeners {
                let url_string = listener_url.to_string();
                
                if Self::is_our_listener(&url_string, &global_ctx) {
                    continue;
                }

                match super::create_connector_by_url(
                    &url_string,
                    &global_ctx,
                    crate::tunnel::IpVersion::Both,
                ).await {
                    Ok(mut connector) => {
                        let peer_mgr_clone = peer_mgr.clone();
                        tokio::spawn(async move {
                            match peer_mgr_clone.try_direct_connect(connector).await {
                                Ok((peer_id, conn_id)) => {
                                    tracing::info!(
                                        ?peer_id,
                                        ?conn_id,
                                        ?url_string,
                                        "Successfully connected to discovered peer"
                                    );
                                }
                                Err(e) => {
                                    tracing::debug!(?e, ?url_string, "Failed to connect to discovered peer");
                                }
                            }
                        });
                        break; // Try only one listener for immediate connection
                    }
                    Err(e) => {
                        tracing::debug!(?e, ?listener_url, "Failed to create connector for discovered peer");
                    }
                }
            }
        }
        tracing::warn!("Multicast discovery peer receiver closed");
    }

    fn is_our_listener(url_string: &str, global_ctx: &ArcGlobalCtx) -> bool {
        let our_listeners = global_ctx.get_running_listeners();
        our_listeners.iter().any(|our_url| {
            // Check if the URL matches any of our listeners
            // This is a simple string comparison, but could be made more sophisticated
            our_url.to_string() == url_string ||
            // Also check if it's the same host:port but different scheme
            Self::same_host_port(our_url, url_string)
        })
    }

    fn same_host_port(our_url: &url::Url, other_url: &str) -> bool {
        if let Ok(other_parsed) = url::Url::parse(other_url) {
            our_url.host() == other_parsed.host() && our_url.port() == other_parsed.port()
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
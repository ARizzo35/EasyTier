#!/bin/bash

# Test script for multicast discovery between two nodes
set -e

echo "Building easytier-core..."
cargo build --bin easytier-core

# Kill any existing processes
pkill -f easytier-core || true
sleep 2

echo "Starting first EasyTier node (node1)..."
./target/debug/easytier-core \
  --network-name test_multicast_discovery \
  --hostname node1 \
  --ipv4 10.144.144.1 \
  --enable-multicast-discovery \
  --no-tun \
  --listeners tcp://0.0.0.0:11020 \
  --console-log-level debug > /tmp/node1.log 2>&1 &
NODE1_PID=$!

sleep 3

echo "Starting second EasyTier node (node2)..."
./target/debug/easytier-core \
  --network-name test_multicast_discovery \
  --hostname node2 \
  --ipv4 10.144.144.2 \
  --enable-multicast-discovery \
  --no-tun \
  --listeners tcp://0.0.0.0:11021 \
  --console-log-level debug > /tmp/node2.log 2>&1 &
NODE2_PID=$!

echo "Waiting for nodes to start and discover each other..."
sleep 10

echo ""
echo "=== Checking for discovery messages between nodes ==="

echo ""
echo "Node1 discovery logs:"
grep -i "multicast\|discovery\|new peer" /tmp/node1.log | tail -n 15 || echo "No discovery messages in node1"

echo ""
echo "Node2 discovery logs:"
grep -i "multicast\|discovery\|new peer" /tmp/node2.log | tail -n 15 || echo "No discovery messages in node2"

echo ""
echo "=== Checking for peer connections ==="

echo ""
echo "Node1 peer connection logs:"
grep -i "peer.*added\|connection.*accepted\|connecting\|connected" /tmp/node1.log | tail -n 10 || echo "No peer connection messages in node1"

echo ""
echo "Node2 peer connection logs:"
grep -i "peer.*added\|connection.*accepted\|connecting\|connected" /tmp/node2.log | tail -n 10 || echo "No peer connection messages in node2"

echo ""
echo "=== Full discovery message analysis ==="

echo ""
echo "Node1 discovery analysis:"
grep -E "(Discovery|Multicast|Successfully sent|Received.*discovery|New peer discovered)" /tmp/node1.log || echo "No detailed discovery logs in node1"

echo ""
echo "Node2 discovery analysis:"
grep -E "(Discovery|Multicast|Successfully sent|Received.*discovery|New peer discovered)" /tmp/node2.log || echo "No detailed discovery logs in node2"

echo ""
echo "=== Cleanup ==="
kill $NODE1_PID $NODE2_PID 2>/dev/null || true
sleep 2
pkill -f easytier-core || true

echo "Test complete!"
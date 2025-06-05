#!/bin/bash

# Test script for multicast discovery functionality
set -e

echo "Building easytier-core..."
cargo build --bin easytier-core

# Kill any existing processes
pkill -f easytier-core || true
sleep 2

echo "Starting first EasyTier node..."
./target/debug/easytier-core --network-name test_multicast_discovery --hostname node1 --ipv4 10.144.144.1 --enable-multicast-discovery --no-tun --listeners tcp://0.0.0.0:11020 > /tmp/node1.log 2>&1 &
NODE1_PID=$!

echo "Starting second EasyTier node..."
./target/debug/easytier-core --network-name test_multicast_discovery --hostname node2 --ipv4 10.144.144.2 --enable-multicast-discovery --no-tun --listeners tcp://0.0.0.0:11021 > /tmp/node2.log 2>&1 &
NODE2_PID=$!

echo "Waiting for nodes to start and discover each other..."
sleep 15

echo "Checking node1 logs:"
echo "=== Node1 Logs ==="
tail -n 20 /tmp/node1.log

echo ""
echo "=== Node2 Logs ==="
tail -n 20 /tmp/node2.log

echo ""
echo "Testing peer connectivity..."

# Try to ping from node1 to node2
echo "Attempting ping from node1 (10.144.144.1) to node2 (10.144.144.2)..."
timeout 5 ping -c 3 10.144.144.2 || echo "Ping failed or timed out"

echo ""
echo "Checking for multicast discovery messages in logs..."
echo "=== Node1 Discovery Messages ==="
grep -i "multicast\|discovery" /tmp/node1.log | tail -n 10 || echo "No discovery messages found in node1"

echo ""
echo "=== Node2 Discovery Messages ==="
grep -i "multicast\|discovery" /tmp/node2.log | tail -n 10 || echo "No discovery messages found in node2"

echo ""
echo "Cleaning up..."
kill $NODE1_PID $NODE2_PID 2>/dev/null || true
sleep 2
pkill -f easytier-core || true

echo "Test complete!"
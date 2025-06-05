#!/bin/bash

# Final test for multicast discovery functionality
set -e

echo "Building easytier-core..."
cargo build --bin easytier-core

# Kill any existing processes and clean up
pkill -f easytier-core || true
sleep 3
rm -f /tmp/node*.log

echo ""
echo "=== Testing Multicast Discovery Between Two Nodes ==="
echo ""

echo "Starting first EasyTier node (node1) on port 11020..."
timeout 30 ./target/debug/easytier-core \
  --network-name test_multicast_discovery \
  --hostname node1 \
  --ipv4 10.144.144.1 \
  --enable-multicast-discovery \
  --no-tun \
  --listeners tcp://0.0.0.0:11020 \
  --console-log-level debug > /tmp/node1.log 2>&1 &
NODE1_PID=$!

echo "Waiting for node1 to start and begin discovery..."
sleep 5

echo "Starting second EasyTier node (node2) on port 11021..."
timeout 30 ./target/debug/easytier-core \
  --network-name test_multicast_discovery \
  --hostname node2 \
  --ipv4 10.144.144.2 \
  --enable-multicast-discovery \
  --no-tun \
  --listeners tcp://0.0.0.0:11021 \
  --console-log-level debug > /tmp/node2.log 2>&1 &
NODE2_PID=$!

echo "Waiting for both nodes to discover each other..."
sleep 10

echo ""
echo "=== Analysis of Multicast Discovery ==="

echo ""
echo "Node1 multicast discovery activity:"
if grep -E "(multicast|discovery|New peer discovered)" /tmp/node1.log; then
    echo "✓ Node1 has multicast discovery activity"
else
    echo "✗ No multicast discovery activity in node1"
fi

echo ""
echo "Node2 multicast discovery activity:"
if grep -E "(multicast|discovery|New peer discovered)" /tmp/node2.log; then
    echo "✓ Node2 has multicast discovery activity"
else
    echo "✗ No multicast discovery activity in node2"
fi

echo ""
echo "=== Checking Address Translation Fix ==="

echo ""
echo "Node1 connectable listeners in discovery messages:"
grep -E "connectable_listeners.*192\.168\." /tmp/node1.log | tail -n 3 || echo "No connectable listener logs found"

echo ""
echo "Node2 connectable listeners in discovery messages:"
grep -E "connectable_listeners.*192\.168\." /tmp/node2.log | tail -n 3 || echo "No connectable listener logs found"

echo ""
echo "=== Discovery Message Exchange ==="

echo ""
echo "Messages sent by node1:"
grep -E "(Successfully sent.*discovery|Sending.*discovery)" /tmp/node1.log | wc -l | xargs echo "Discovery messages sent:"

echo ""
echo "Messages received by node1:"
grep -E "(Received.*discovery)" /tmp/node1.log | wc -l | xargs echo "Discovery messages received:"

echo ""
echo "Messages sent by node2:"
grep -E "(Successfully sent.*discovery|Sending.*discovery)" /tmp/node2.log | wc -l | xargs echo "Discovery messages sent:"

echo ""
echo "Messages received by node2:"
grep -E "(Received.*discovery)" /tmp/node2.log | wc -l | xargs echo "Discovery messages received:"

echo ""
echo "=== Connection Attempts After Discovery ==="

echo ""
echo "Node1 connection attempts:"
grep -E "(connecting|connected|Failed to connect|Successfully connected)" /tmp/node1.log | tail -n 5 || echo "No connection attempts in node1"

echo ""
echo "Node2 connection attempts:"
grep -E "(connecting|connected|Failed to connect|Successfully connected)" /tmp/node2.log | tail -n 5 || echo "No connection attempts in node2"

echo ""
echo "=== Final Status ==="

# Check if nodes discovered each other
if grep -q "New peer discovered" /tmp/node1.log; then
    echo "✓ Node1 discovered peers via multicast"
else
    echo "✗ Node1 did not discover any peers"
fi

if grep -q "New peer discovered" /tmp/node2.log; then
    echo "✓ Node2 discovered peers via multicast"  
else
    echo "✗ Node2 did not discover any peers"
fi

# Check if connection was successful
if grep -q "Successfully connected" /tmp/node1.log || grep -q "Successfully connected" /tmp/node2.log; then
    echo "✓ Peer connection established successfully"
else
    echo "✗ No successful peer connections found"
fi

echo ""
echo "=== Cleanup ==="
kill $NODE1_PID $NODE2_PID 2>/dev/null || true
sleep 2
pkill -f easytier-core || true

echo ""
echo "Test complete! Check logs at /tmp/node1.log and /tmp/node2.log for details."
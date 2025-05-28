package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/celestiaorg/celestia-node/nodebuilder/node"
	"github.com/celestiaorg/celestia-node/share/shwap/p2p/discovery"
)

func TestNewDHTMetrics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a test DHT instance
	dhtInstance := createTestDHT(ctx, t)
	defer dhtInstance.Close()

	// Test successful creation
	metrics, err := NewDHTMetrics(dhtInstance)
	require.NoError(t, err)
	require.NotNil(t, metrics)
	assert.Equal(t, dhtInstance, metrics.dht)
	assert.NotNil(t, metrics.clientReg)

	// Test cleanup
	err = metrics.Close()
	require.NoError(t, err)
}

func TestNewDHTMetrics_NilDHT(t *testing.T) {
	// Test with nil DHT - should still create metrics but with limited functionality
	metrics, err := NewDHTMetrics(nil)
	require.NoError(t, err)
	require.NotNil(t, metrics)
	assert.Nil(t, metrics.dht)

	// Test cleanup with nil DHT
	err = metrics.Close()
	require.NoError(t, err)
}

func TestDHTMetrics_ObserveRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dhtInstance := createTestDHT(ctx, t)
	defer dhtInstance.Close()

	metrics, err := NewDHTMetrics(dhtInstance)
	require.NoError(t, err)
	defer metrics.Close()

	// Test observing successful request
	metrics.ObserveRequest(ctx, "find_peer", 100*time.Millisecond, false)

	// Test observing failed request
	metrics.ObserveRequest(ctx, "find_peer", 200*time.Millisecond, true)

	// Test with different request types
	requestTypes := []string{"get_value", "put_value", "find_providers", "provide"}
	for _, reqType := range requestTypes {
		metrics.ObserveRequest(ctx, reqType, 50*time.Millisecond, false)
	}

	// Test with nil metrics (should not panic)
	var nilMetrics *DHTMetrics
	nilMetrics.ObserveRequest(ctx, "test", time.Second, false)
}

func TestDHTMetrics_ObserveQuery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dhtInstance := createTestDHT(ctx, t)
	defer dhtInstance.Close()

	metrics, err := NewDHTMetrics(dhtInstance)
	require.NoError(t, err)
	defer metrics.Close()

	// Test different query types
	queryTypes := []string{"find_peer", "find_providers", "get_closest_peers"}
	for _, queryType := range queryTypes {
		metrics.ObserveQuery(ctx, queryType)
	}

	// Test with nil metrics (should not panic)
	var nilMetrics *DHTMetrics
	nilMetrics.ObserveQuery(ctx, "test")
}

func TestDHTMetrics_ObserveProvide(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dhtInstance := createTestDHT(ctx, t)
	defer dhtInstance.Close()

	metrics, err := NewDHTMetrics(dhtInstance)
	require.NoError(t, err)
	defer metrics.Close()

	// Test multiple provide operations
	for i := 0; i < 5; i++ {
		metrics.ObserveProvide(ctx)
	}

	// Test with nil metrics (should not panic)
	var nilMetrics *DHTMetrics
	nilMetrics.ObserveProvide(ctx)
}

func TestDHTMetrics_ObservePutValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dhtInstance := createTestDHT(ctx, t)
	defer dhtInstance.Close()

	metrics, err := NewDHTMetrics(dhtInstance)
	require.NoError(t, err)
	defer metrics.Close()

	// Test multiple put value operations
	for i := 0; i < 3; i++ {
		metrics.ObservePutValue(ctx)
	}

	// Test with nil metrics (should not panic)
	var nilMetrics *DHTMetrics
	nilMetrics.ObservePutValue(ctx)
}

func TestDHTMetrics_ObserveGetValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dhtInstance := createTestDHT(ctx, t)
	defer dhtInstance.Close()

	metrics, err := NewDHTMetrics(dhtInstance)
	require.NoError(t, err)
	defer metrics.Close()

	// Test multiple get value operations
	for i := 0; i < 7; i++ {
		metrics.ObserveGetValue(ctx)
	}

	// Test with nil metrics (should not panic)
	var nilMetrics *DHTMetrics
	nilMetrics.ObserveGetValue(ctx)
}

func TestDHTMetrics_ObserveError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dhtInstance := createTestDHT(ctx, t)
	defer dhtInstance.Close()

	metrics, err := NewDHTMetrics(dhtInstance)
	require.NoError(t, err)
	defer metrics.Close()

	// Test different error types
	errorTypes := []string{"network_error", "timeout", "invalid_response", "peer_not_found"}
	for _, errorType := range errorTypes {
		metrics.ObserveError(ctx, errorType)
	}

	// Test with nil metrics (should not panic)
	var nilMetrics *DHTMetrics
	nilMetrics.ObserveError(ctx, "test_error")
}

func TestDHTMetrics_ObserveTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dhtInstance := createTestDHT(ctx, t)
	defer dhtInstance.Close()

	metrics, err := NewDHTMetrics(dhtInstance)
	require.NoError(t, err)
	defer metrics.Close()

	// Test different operations that can timeout
	operations := []string{"find_peer", "get_value", "put_value", "provide"}
	for _, operation := range operations {
		metrics.ObserveTimeout(ctx, operation)
	}

	// Test with nil metrics (should not panic)
	var nilMetrics *DHTMetrics
	nilMetrics.ObserveTimeout(ctx, "test_operation")
}

func TestDHTMetrics_ObservePeerLatency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dhtInstance := createTestDHT(ctx, t)
	defer dhtInstance.Close()

	metrics, err := NewDHTMetrics(dhtInstance)
	require.NoError(t, err)
	defer metrics.Close()

	// Create test peer IDs
	testPeers := generateTestPeerIDs(t, 3)

	// Test observing latency for different peers
	latencies := []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond}
	for i, peerID := range testPeers {
		metrics.ObservePeerLatency(ctx, peerID, latencies[i])
	}

	// Test with nil metrics (should not panic)
	var nilMetrics *DHTMetrics
	nilMetrics.ObservePeerLatency(ctx, testPeers[0], time.Second)
}

func TestDHTMetrics_ContextHandling(t *testing.T) {
	dhtInstance := createTestDHT(context.Background(), t)
	defer dhtInstance.Close()

	metrics, err := NewDHTMetrics(dhtInstance)
	require.NoError(t, err)
	defer metrics.Close()

	// Test with canceled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	// These should not panic and should handle the canceled context gracefully
	metrics.ObserveRequest(canceledCtx, "test", time.Millisecond, false)
	metrics.ObserveQuery(canceledCtx, "test")
	metrics.ObserveProvide(canceledCtx)
	metrics.ObservePutValue(canceledCtx)
	metrics.ObserveGetValue(canceledCtx)
	metrics.ObserveError(canceledCtx, "test")
	metrics.ObserveTimeout(canceledCtx, "test")

	testPeer := generateTestPeerIDs(t, 1)[0]
	metrics.ObservePeerLatency(canceledCtx, testPeer, time.Millisecond)
}

func TestDHTMetrics_ConcurrentAccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dhtInstance := createTestDHT(ctx, t)
	defer dhtInstance.Close()

	metrics, err := NewDHTMetrics(dhtInstance)
	require.NoError(t, err)
	defer metrics.Close()

	// Test concurrent access to metrics
	const numGoroutines = 10
	const numOperations = 100

	done := make(chan struct{}, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer func() { done <- struct{}{} }()

			for j := 0; j < numOperations; j++ {
				switch j % 7 {
				case 0:
					metrics.ObserveRequest(ctx, "concurrent_test", time.Millisecond, false)
				case 1:
					metrics.ObserveQuery(ctx, "concurrent_test")
				case 2:
					metrics.ObserveProvide(ctx)
				case 3:
					metrics.ObservePutValue(ctx)
				case 4:
					metrics.ObserveGetValue(ctx)
				case 5:
					metrics.ObserveError(ctx, "concurrent_error")
				case 6:
					metrics.ObserveTimeout(ctx, "concurrent_timeout")
				}
			}
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("Timeout waiting for concurrent operations to complete")
		}
	}
}

func TestDHTMetrics_Close(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dhtInstance := createTestDHT(ctx, t)
	defer dhtInstance.Close()

	metrics, err := NewDHTMetrics(dhtInstance)
	require.NoError(t, err)

	// Test normal close
	err = metrics.Close()
	require.NoError(t, err)

	// Test double close (should not error)
	err = metrics.Close()
	require.NoError(t, err)

	// Test close with nil metrics
	var nilMetrics *DHTMetrics
	err = nilMetrics.Close()
	require.NoError(t, err)
}

func TestDHTMetrics_ObservableGauges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a mock network with multiple peers to test routing table metrics
	net, err := mocknet.FullMeshConnected(5)
	require.NoError(t, err)

	host := net.Hosts()[0]
	datastore := dssync.MutexWrap(datastore.NewMapDatastore())

	// Create DHT with server mode to have a routing table
	dhtInstance, err := discovery.NewDHT(ctx, "test", nil, host, datastore, dht.ModeServer)
	require.NoError(t, err)
	defer dhtInstance.Close()

	// Bootstrap the DHT
	err = dhtInstance.Bootstrap(ctx)
	require.NoError(t, err)

	metrics, err := NewDHTMetrics(dhtInstance)
	require.NoError(t, err)
	defer metrics.Close()

	// Give some time for the DHT to populate its routing table
	time.Sleep(2 * time.Second)

	// The observable gauges should be working through the callback mechanism
	// We can't directly test the values without setting up a full metrics collection system,
	// but we can verify that the metrics were created successfully and the callback doesn't panic
	assert.NotNil(t, metrics.routingTableSize)
	assert.NotNil(t, metrics.bucketSizes)
	assert.NotNil(t, metrics.connectedPeers)
	assert.NotNil(t, metrics.activeRequests)
}

func TestDHTMetrics_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a test scenario that simulates real DHT server load
	dhtInstance := createTestDHT(ctx, t)
	defer dhtInstance.Close()

	metrics, err := NewDHTMetrics(dhtInstance)
	require.NoError(t, err)
	defer metrics.Close()

	// Simulate a series of DHT operations that would happen during server load
	scenarios := []struct {
		name      string
		operation func(t *testing.T)
	}{
		{
			name: "find_peer_requests",
			operation: func(t *testing.T) {
				for i := 0; i < 10; i++ {
					duration := time.Duration(50+i*10) * time.Millisecond
					failed := i%3 == 0 // Some requests fail
					metrics.ObserveRequest(ctx, "find_peer", duration, failed)
					if failed {
						metrics.ObserveError(ctx, "peer_not_found")
					}
				}
			},
		},
		{
			name: "provide_operations",
			operation: func(t *testing.T) {
				for i := 0; i < 5; i++ {
					metrics.ObserveProvide(ctx)
					metrics.ObserveRequest(ctx, "provide", 200*time.Millisecond, false)
				}
			},
		},
		{
			name: "value_operations",
			operation: func(t *testing.T) {
				for i := 0; i < 8; i++ {
					if i%2 == 0 {
						metrics.ObservePutValue(ctx)
						metrics.ObserveRequest(ctx, "put_value", 150*time.Millisecond, false)
					} else {
						metrics.ObserveGetValue(ctx)
						metrics.ObserveRequest(ctx, "get_value", 100*time.Millisecond, i%4 == 1)
						if i%4 == 1 {
							metrics.ObserveError(ctx, "value_not_found")
						}
					}
				}
			},
		},
		{
			name: "query_operations",
			operation: func(t *testing.T) {
				queryTypes := []string{"find_peer", "find_providers", "get_closest_peers"}
				for _, queryType := range queryTypes {
					for j := 0; j < 3; j++ {
						metrics.ObserveQuery(ctx, queryType)
						if j == 2 { // Last query times out
							metrics.ObserveTimeout(ctx, queryType)
						}
					}
				}
			},
		},
		{
			name: "peer_latency_tracking",
			operation: func(t *testing.T) {
				testPeers := generateTestPeerIDs(t, 5)
				latencies := []time.Duration{
					30 * time.Millisecond,
					75 * time.Millisecond,
					120 * time.Millisecond,
					200 * time.Millisecond,
					350 * time.Millisecond,
				}
				for i, peerID := range testPeers {
					metrics.ObservePeerLatency(ctx, peerID, latencies[i])
				}
			},
		},
	}

	// Execute all scenarios
	for _, scenario := range scenarios {
		t.Run(scenario.name, scenario.operation)
	}

	// Verify that all operations completed without panics
	// In a real test environment with metrics collection, we would verify the actual metric values
}

// Helper functions

func createTestDHT(ctx context.Context, t *testing.T) *dht.IpfsDHT {
	net, err := mocknet.FullMeshConnected(1)
	require.NoError(t, err)

	host := net.Hosts()[0]
	datastore := dssync.MutexWrap(datastore.NewMapDatastore())

	dhtInstance, err := discovery.NewDHT(ctx, "test", nil, host, datastore, dht.ModeServer)
	require.NoError(t, err)

	return dhtInstance
}

func generateTestPeerIDs(t *testing.T, count int) []peer.ID {
	net, err := mocknet.FullMeshConnected(count)
	require.NoError(t, err)

	peerIDs := make([]peer.ID, count)
	for i, host := range net.Hosts() {
		peerIDs[i] = host.ID()
	}

	return peerIDs
}

// Benchmark tests to ensure metrics don't significantly impact performance

func BenchmarkDHTMetrics_ObserveRequest(b *testing.B) {
	ctx := context.Background()
	dhtInstance := createTestDHTForBenchmark(ctx, b)
	defer dhtInstance.Close()

	metrics, err := NewDHTMetrics(dhtInstance)
	require.NoError(b, err)
	defer metrics.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			metrics.ObserveRequest(ctx, "benchmark", time.Millisecond, false)
		}
	})
}

func BenchmarkDHTMetrics_ObserveQuery(b *testing.B) {
	ctx := context.Background()
	dhtInstance := createTestDHTForBenchmark(ctx, b)
	defer dhtInstance.Close()

	metrics, err := NewDHTMetrics(dhtInstance)
	require.NoError(b, err)
	defer metrics.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			metrics.ObserveQuery(ctx, "benchmark")
		}
	})
}

func BenchmarkDHTMetrics_ObservePeerLatency(b *testing.B) {
	ctx := context.Background()
	dhtInstance := createTestDHTForBenchmark(ctx, b)
	defer dhtInstance.Close()

	metrics, err := NewDHTMetrics(dhtInstance)
	require.NoError(b, err)
	defer metrics.Close()

	testPeer := generateTestPeerIDsForBenchmark(b, 1)[0]

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			metrics.ObservePeerLatency(ctx, testPeer, time.Millisecond)
		}
	})
}

// Helper functions for benchmarks

func createTestDHTForBenchmark(ctx context.Context, b *testing.B) *dht.IpfsDHT {
	net, err := mocknet.FullMeshConnected(1)
	require.NoError(b, err)

	host := net.Hosts()[0]
	datastore := dssync.MutexWrap(datastore.NewMapDatastore())

	dhtInstance, err := discovery.NewDHT(ctx, "test", nil, host, datastore, dht.ModeServer)
	require.NoError(b, err)

	return dhtInstance
}

func generateTestPeerIDsForBenchmark(b *testing.B, count int) []peer.ID {
	net, err := mocknet.FullMeshConnected(count)
	require.NoError(b, err)

	peerIDs := make([]peer.ID, count)
	for i, host := range net.Hosts() {
		peerIDs[i] = host.ID()
	}

	return peerIDs
}

// Integration tests for DHT metrics with P2P module

func TestNewDHTMetrics_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tests := []struct {
		name     string
		nodeType node.Type
		dht      *dht.IpfsDHT
		wantNil  bool
	}{
		{
			name:     "full_node_with_dht",
			nodeType: node.Full,
			dht:      createTestDHT(ctx, t),
			wantNil:  false,
		},
		{
			name:     "bridge_node_with_dht",
			nodeType: node.Bridge,
			dht:      createTestDHT(ctx, t),
			wantNil:  false,
		},
		{
			name:     "light_node_with_dht",
			nodeType: node.Light,
			dht:      createTestDHT(ctx, t),
			wantNil:  true, // Light nodes should not have DHT server metrics
		},
		{
			name:     "full_node_without_dht",
			nodeType: node.Full,
			dht:      nil,
			wantNil:  true, // No DHT means no metrics
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.dht != nil {
				defer tt.dht.Close()
			}

			metrics, err := newDHTMetrics(tt.dht, tt.nodeType)
			require.NoError(t, err)

			if tt.wantNil {
				assert.Nil(t, metrics)
			} else {
				assert.NotNil(t, metrics)
				assert.Equal(t, tt.dht, metrics.dht)
				// Clean up
				err = metrics.Close()
				require.NoError(t, err)
			}
		})
	}
}

func TestNewDHTMetrics_InvalidNodeType(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dhtInstance := createTestDHT(ctx, t)
	defer dhtInstance.Close()

	// Test with invalid node type (0 is invalid according to node.Type definition)
	invalidNodeType := node.Type(0)
	metrics, err := newDHTMetrics(dhtInstance, invalidNodeType)
	require.Error(t, err)
	assert.Nil(t, metrics)
	assert.Contains(t, err.Error(), "unsupported node type")
}

func TestDHTMetrics_RealWorldScenario(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a more realistic DHT setup with multiple peers
	net, err := mocknet.FullMeshConnected(3)
	require.NoError(t, err)

	// Set up a DHT server node
	host := net.Hosts()[0]
	datastore := dssync.MutexWrap(datastore.NewMapDatastore())
	dhtInstance, err := discovery.NewDHT(ctx, "test", nil, host, datastore, dht.ModeServer)
	require.NoError(t, err)
	defer dhtInstance.Close()

	// Bootstrap the DHT
	err = dhtInstance.Bootstrap(ctx)
	require.NoError(t, err)

	// Create metrics for a Full node
	metrics, err := newDHTMetrics(dhtInstance, node.Full)
	require.NoError(t, err)
	require.NotNil(t, metrics)
	defer metrics.Close()

	// Simulate real DHT server operations
	scenarios := []struct {
		name      string
		operation func()
	}{
		{
			name: "handle_find_peer_requests",
			operation: func() {
				for i := 0; i < 5; i++ {
					duration := time.Duration(50+i*20) * time.Millisecond
					failed := i%4 == 0
					metrics.ObserveRequest(ctx, "find_peer", duration, failed)
					if failed {
						metrics.ObserveError(ctx, "peer_not_found")
					}
				}
			},
		},
		{
			name: "handle_provide_requests",
			operation: func() {
				for i := 0; i < 3; i++ {
					metrics.ObserveProvide(ctx)
					metrics.ObserveRequest(ctx, "provide", 150*time.Millisecond, false)
				}
			},
		},
		{
			name: "handle_value_requests",
			operation: func() {
				// Simulate get/put value operations
				for i := 0; i < 4; i++ {
					if i%2 == 0 {
						metrics.ObservePutValue(ctx)
						metrics.ObserveRequest(ctx, "put_value", 100*time.Millisecond, false)
					} else {
						metrics.ObserveGetValue(ctx)
						failed := i == 3
						metrics.ObserveRequest(ctx, "get_value", 80*time.Millisecond, failed)
						if failed {
							metrics.ObserveError(ctx, "value_not_found")
						}
					}
				}
			},
		},
		{
			name: "track_peer_latencies",
			operation: func() {
				// Simulate latency tracking for connected peers
				connectedPeers := net.Hosts()[1:]
				latencies := []time.Duration{45 * time.Millisecond, 120 * time.Millisecond}
				for i, peer := range connectedPeers {
					metrics.ObservePeerLatency(ctx, peer.ID(), latencies[i])
				}
			},
		},
		{
			name: "handle_timeouts_and_errors",
			operation: func() {
				// Simulate various timeout scenarios
				operations := []string{"find_peer", "get_value", "provide"}
				for _, op := range operations {
					metrics.ObserveTimeout(ctx, op)
				}

				// Simulate various error scenarios
				errorTypes := []string{"network_error", "invalid_response", "rate_limited"}
				for _, errType := range errorTypes {
					metrics.ObserveError(ctx, errType)
				}
			},
		},
	}

	// Execute all scenarios to simulate real server load
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			scenario.operation()
		})
	}

	// Give some time for observable gauges to be updated
	time.Sleep(100 * time.Millisecond)

	// Verify that the metrics instance is still functional
	assert.NotNil(t, metrics.dht)
	assert.NotNil(t, metrics.clientReg)
}

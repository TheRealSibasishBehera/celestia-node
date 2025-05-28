package p2p

import (
	"context"
	"fmt"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/fx"

	"github.com/celestiaorg/celestia-node/libs/utils"
	"github.com/celestiaorg/celestia-node/nodebuilder/node"
)

const (
	requestTypeLabel = "request_type"
	statusLabel      = "status"
	failedLabel      = "failed"
)

var dhtMeter = otel.Meter("dht_server")

// DHTMetrics tracks DHT server performance and load metrics
type DHTMetrics struct {
	// Request metrics
	totalRequests   metric.Int64Counter
	requestDuration metric.Float64Histogram
	activeRequests  metric.Int64ObservableGauge

	// Routing table metrics
	routingTableSize metric.Int64ObservableGauge
	bucketSizes      metric.Int64ObservableGauge

	// Peer metrics
	connectedPeers metric.Int64ObservableGauge
	peerLatency    metric.Float64Histogram

	// Server load metrics
	queryLoad    metric.Int64Counter
	provideLoad  metric.Int64Counter
	putValueLoad metric.Int64Counter
	getValueLoad metric.Int64Counter

	// Error metrics
	errorCount   metric.Int64Counter
	timeoutCount metric.Int64Counter

	dht       *dht.IpfsDHT
	clientReg metric.Registration
}

// NewDHTMetrics creates a new DHT metrics instance
func NewDHTMetrics(dhtInstance *dht.IpfsDHT) (*DHTMetrics, error) {
	totalRequests, err := dhtMeter.Int64Counter("dht_server_total_requests",
		metric.WithDescription("Total number of DHT requests handled by the server"))
	if err != nil {
		return nil, err
	}

	requestDuration, err := dhtMeter.Float64Histogram("dht_server_request_duration_seconds",
		metric.WithDescription("Duration of DHT requests in seconds"))
	if err != nil {
		return nil, err
	}

	activeRequests, err := dhtMeter.Int64ObservableGauge("dht_server_active_requests",
		metric.WithDescription("Number of currently active DHT requests"))
	if err != nil {
		return nil, err
	}

	routingTableSize, err := dhtMeter.Int64ObservableGauge("dht_server_routing_table_size",
		metric.WithDescription("Total number of peers in the routing table"))
	if err != nil {
		return nil, err
	}

	bucketSizes, err := dhtMeter.Int64ObservableGauge("dht_server_bucket_sizes",
		metric.WithDescription("Number of peers in each routing table bucket"))
	if err != nil {
		return nil, err
	}

	connectedPeers, err := dhtMeter.Int64ObservableGauge("dht_server_connected_peers",
		metric.WithDescription("Number of currently connected peers"))
	if err != nil {
		return nil, err
	}

	peerLatency, err := dhtMeter.Float64Histogram("dht_server_peer_latency_seconds",
		metric.WithDescription("Latency to DHT peers in seconds"))
	if err != nil {
		return nil, err
	}

	queryLoad, err := dhtMeter.Int64Counter("dht_server_query_requests",
		metric.WithDescription("Number of query requests handled"))
	if err != nil {
		return nil, err
	}

	provideLoad, err := dhtMeter.Int64Counter("dht_server_provide_requests",
		metric.WithDescription("Number of provide requests handled"))
	if err != nil {
		return nil, err
	}

	putValueLoad, err := dhtMeter.Int64Counter("dht_server_put_value_requests",
		metric.WithDescription("Number of put value requests handled"))
	if err != nil {
		return nil, err
	}

	getValueLoad, err := dhtMeter.Int64Counter("dht_server_get_value_requests",
		metric.WithDescription("Number of get value requests handled"))
	if err != nil {
		return nil, err
	}

	errorCount, err := dhtMeter.Int64Counter("dht_server_errors",
		metric.WithDescription("Number of DHT server errors"))
	if err != nil {
		return nil, err
	}

	timeoutCount, err := dhtMeter.Int64Counter("dht_server_timeouts",
		metric.WithDescription("Number of DHT request timeouts"))
	if err != nil {
		return nil, err
	}

	metrics := &DHTMetrics{
		totalRequests:    totalRequests,
		requestDuration:  requestDuration,
		activeRequests:   activeRequests,
		routingTableSize: routingTableSize,
		bucketSizes:      bucketSizes,
		connectedPeers:   connectedPeers,
		peerLatency:      peerLatency,
		queryLoad:        queryLoad,
		provideLoad:      provideLoad,
		putValueLoad:     putValueLoad,
		getValueLoad:     getValueLoad,
		errorCount:       errorCount,
		timeoutCount:     timeoutCount,
		dht:              dhtInstance,
	}

	// Register callback for observable gauges
	callback := func(_ context.Context, observer metric.Observer) error {
		if metrics.dht == nil {
			return nil
		}

		// Routing table metrics
		rt := metrics.dht.RoutingTable()
		if rt != nil {
			observer.ObserveInt64(routingTableSize, int64(rt.Size()))

			// Bucket sizes - iterate through Common Prefix Lengths (CPLs)
			// In Kademlia, typically we have up to 256 buckets (for 256-bit keys)
			// but we'll check the first 32 CPLs which should cover most practical cases
			for cpl := uint(0); cpl < 32; cpl++ {
				peerCount := rt.NPeersForCpl(cpl)
				if peerCount > 0 {
					observer.ObserveInt64(bucketSizes, int64(peerCount),
						metric.WithAttributes(attribute.Int("cpl", int(cpl))))
				}
			}
		}

		// Connected peers
		host := metrics.dht.Host()
		if host != nil {
			connectedPeerCount := len(host.Network().Peers())
			observer.ObserveInt64(connectedPeers, int64(connectedPeerCount))
		}

		return nil
	}

	metrics.clientReg, err = dhtMeter.RegisterCallback(callback,
		activeRequests, routingTableSize, bucketSizes, connectedPeers)
	if err != nil {
		return nil, fmt.Errorf("registering DHT metrics callback: %w", err)
	}

	return metrics, nil
}

// ObserveRequest records metrics for a DHT request
func (m *DHTMetrics) ObserveRequest(ctx context.Context, requestType string, duration time.Duration, failed bool) {
	if m == nil {
		return
	}

	ctx = utils.ResetContextOnError(ctx)

	m.totalRequests.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String(requestTypeLabel, requestType),
			attribute.Bool(failedLabel, failed),
		))

	m.requestDuration.Record(ctx, duration.Seconds(),
		metric.WithAttributes(
			attribute.String(requestTypeLabel, requestType),
			attribute.Bool(failedLabel, failed),
		))
}

// ObserveQuery records metrics for query operations
func (m *DHTMetrics) ObserveQuery(ctx context.Context, queryType string) {
	if m == nil {
		return
	}

	ctx = utils.ResetContextOnError(ctx)
	m.queryLoad.Add(ctx, 1,
		metric.WithAttributes(attribute.String("query_type", queryType)))
}

// ObserveProvide records metrics for provide operations
func (m *DHTMetrics) ObserveProvide(ctx context.Context) {
	if m == nil {
		return
	}

	ctx = utils.ResetContextOnError(ctx)
	m.provideLoad.Add(ctx, 1)
}

// ObservePutValue records metrics for put value operations
func (m *DHTMetrics) ObservePutValue(ctx context.Context) {
	if m == nil {
		return
	}

	ctx = utils.ResetContextOnError(ctx)
	m.putValueLoad.Add(ctx, 1)
}

// ObserveGetValue records metrics for get value operations
func (m *DHTMetrics) ObserveGetValue(ctx context.Context) {
	if m == nil {
		return
	}

	ctx = utils.ResetContextOnError(ctx)
	m.getValueLoad.Add(ctx, 1)
}

// ObserveError records error metrics
func (m *DHTMetrics) ObserveError(ctx context.Context, errorType string) {
	if m == nil {
		return
	}

	ctx = utils.ResetContextOnError(ctx)
	m.errorCount.Add(ctx, 1,
		metric.WithAttributes(attribute.String("error_type", errorType)))
}

// ObserveTimeout records timeout metrics
func (m *DHTMetrics) ObserveTimeout(ctx context.Context, operation string) {
	if m == nil {
		return
	}

	ctx = utils.ResetContextOnError(ctx)
	m.timeoutCount.Add(ctx, 1,
		metric.WithAttributes(attribute.String("operation", operation)))
}

// ObservePeerLatency records peer latency metrics
func (m *DHTMetrics) ObservePeerLatency(ctx context.Context, peerID peer.ID, latency time.Duration) {
	if m == nil {
		return
	}

	ctx = utils.ResetContextOnError(ctx)
	m.peerLatency.Record(ctx, latency.Seconds(),
		metric.WithAttributes(attribute.String("peer_id", peerID.String())))
}

// Close unregisters the metrics callback
func (m *DHTMetrics) Close() error {
	if m == nil || m.clientReg == nil {
		return nil
	}
	return m.clientReg.Unregister()
}

// WithDHTMetrics provides DHT metrics integration for the P2P module.
// DHT metrics are only enabled for server nodes (Full and Bridge) since Light nodes
// run DHT in client mode and don't serve requests.
func WithDHTMetrics() fx.Option {
	return fx.Options(
		fx.Provide(newDHTMetrics),
		fx.Invoke(func(lc fx.Lifecycle, metrics *DHTMetrics) {
			if metrics != nil {
				lc.Append(fx.Hook{
					OnStop: func(_ context.Context) error {
						return metrics.Close()
					},
				})
			}
		}),
	)
}

// newDHTMetrics creates DHT metrics instance if DHT is available and in server mode
func newDHTMetrics(dhtInstance *dht.IpfsDHT, nodeType node.Type) (*DHTMetrics, error) {
	switch nodeType {
	case node.Full, node.Bridge:
		if dhtInstance != nil {
			return NewDHTMetrics(dhtInstance)
		}
		// If DHT is not available, return nil metrics - we want to allow the system to continue without metrics
		// rather than failing when DHT is not available
		return nil, nil //nolint:nilnil
	case node.Light:
		// Light nodes don't need DHT server metrics
		return nil, nil //nolint:nilnil
	default:
		return nil, fmt.Errorf("unsupported node type for DHT metrics: %s", nodeType)
	}
}

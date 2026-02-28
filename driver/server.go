package driver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var (
	DriverVersion = "0.0.0"
	BuildDate     = "unknown"
)

type Driver struct {
	Controller *ControllerServer
	Node       *NodeServer
	Identity   *IdentityServer

	server *grpc.Server
}

func NewDriver(nodeID string, endpoint string, volumeReadyTimeout, volumeReadyRetryTimeout time.Duration) *Driver {
	logrus.Infof("Starting SolidFire CSI Driver version %s build %s", DriverVersion, BuildDate)
	d := &Driver{
		Controller: NewControllerServer(volumeReadyTimeout, volumeReadyRetryTimeout),
		Node:       NewNodeServer(nodeID),
		Identity:   NewIdentityServer(),
	}
	// If Node was created in the same process (controller-mode), allow it to
	// consult the Controller's session advisory when enabled.
	if d.Node != nil {
		d.Node.Controller = d.Controller
	}
	// Start async registry sweeper in background to GC old entries
	go d.Controller.asyncSweeper()
	return d
}

func (d *Driver) Run(endpoint string) error {
	proto, addr, err := parseEndpoint(endpoint)
	if err != nil {
		return err
	}

	if proto == "unix" {
		addr = "/" + addr
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	listener, err := net.Listen(proto, addr)
	if err != nil {
		return err
	}

	d.server = grpc.NewServer(
		grpc.UnaryInterceptor(logGRPC),
	)

	csi.RegisterIdentityServer(d.server, d.Identity)
	csi.RegisterControllerServer(d.server, d.Controller)
	// Register Node server when running in Node mode (NodeID set).
	// For single-node dev/testing, allow forcing registration via
	// `SFCSI_REGISTER_NODE_IN_CONTROLLER=true` (opt-in).
	registerNode := false
	if d.Node != nil && d.Node.NodeID != "" {
		registerNode = true
	} else if os.Getenv("SFCSI_REGISTER_NODE_IN_CONTROLLER") == "true" {
		registerNode = true
	}

	if registerNode {
		csi.RegisterNodeServer(d.server, d.Node)
		if d.Node != nil && d.Node.NodeID == "" {
			logrus.Warnf("Node service registered in controller (SFCSI_REGISTER_NODE_IN_CONTROLLER=true); host-level ops may fail if controller pod is unprivileged")
		}
	} else {
		logrus.Infof("Node service not registered (controller-only mode)")
	}
	csi.RegisterGroupControllerServer(d.server, d.Controller)

	// Metrics: can be disabled via env `SFCSI_DISABLE_METRICS=true` to avoid
	// running host-level probes (nsenter) from unprivileged controller pods.
	if os.Getenv("SFCSI_DISABLE_METRICS") == "true" {
		logrus.Infof("Metrics collection disabled via SFCSI_DISABLE_METRICS")
	} else {
		// Start Background Metrics Collection (every 5 minutes)
		d.Controller.StartMetricsCollection(5 * time.Minute)
		// Start Node metrics (iSCSI sessions) collection as well
		d.Node.StartMetricsCollection(30 * time.Second)

		// Start Prometheus Metrics Server
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			metricsAddr := ":8080" // TODO: Make configurable
			logrus.Infof("Serving metrics at %s/metrics", metricsAddr)
			if err := http.ListenAndServe(metricsAddr, nil); err != nil {
				logrus.Errorf("Failed to start metrics server: %v", err)
			}
		}()
	}

	logrus.Infof("Listening for connections on address: %#v", listener.Addr())
	return d.server.Serve(listener)
}

func parseEndpoint(ep string) (string, string, error) {
	if ep == "" {
		return "", "", fmt.Errorf("invalid endpoint: %s", ep)
	}
	// simple parsing, assuming "unix:///..." or "tcp://..."
	// but regex or strings.Split is safer
	// ...
	// For now assuming standard CSI socket format
	// unix:///var/lib/kubelet/plugins/.../csi.sock

	// Quick parse
	if len(ep) > 7 && ep[:7] == "unix://" {
		return "unix", ep[7:], nil
	}
	return "tcp", ep, nil
}

func logGRPC(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	logrus.Infof("GRPC call: %s", info.FullMethod)
	logrus.Debugf("GRPC request: %s", protosanitizer.StripSecrets(req))
	resp, err := handler(ctx, req)
	if err != nil {
		logrus.Errorf("GRPC error: %v", err)
	}
	return resp, err
}

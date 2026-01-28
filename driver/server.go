package driver

import (
	"context"
	"fmt"
	"net"
	"os"

	// Add sync for WaitGroup
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Driver struct {
	Controller *ControllerServer
	Node       *NodeServer
	Identity   *IdentityServer

	server *grpc.Server
}

func NewDriver(nodeID string, endpoint string) *Driver {
	return &Driver{
		Controller: NewControllerServer(),
		Node:       NewNodeServer(nodeID),
		Identity:   NewIdentityServer(),
	}
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
	csi.RegisterNodeServer(d.server, d.Node)

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
	resp, err := handler(ctx, req)
	if err != nil {
		logrus.Errorf("GRPC error: %v", err)
	}
	return resp, err
}

package driver

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

type IdentityServer struct {
	csi.UnimplementedIdentityServer
}

func NewIdentityServer() *IdentityServer {
	return &IdentityServer{}
}

func (ids *IdentityServer) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          "csi.solidfire.com",
		VendorVersion: "1.0.0",
	}, nil
}

func (ids *IdentityServer) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
		},
	}, nil
}

func (ids *IdentityServer) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	// We can check if iscsid is running here if needed
	return &csi.ProbeResponse{}, nil
}

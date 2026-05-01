package main

import (
	"fmt"
	"os"
	"time"

	"github.com/scaleoutsean/solidfire-csi/driver"
	"github.com/spf13/cobra"
)

var (
	endpoint                string
	nodeID                  string
	debug                   bool
	volReadyTimeoutStr      string
	volReadyRetryTimeoutStr string
)

func main() {
	var cmd = &cobra.Command{
		Use:   "solidfire-csi",
		Short: "SolidFire CSI Driver",
		Run: func(cmd *cobra.Command, args []string) {
			if debug {
				os.Setenv("SFCSI_DEBUG", "true")
			}
			// Parse duration flags (defaults validated by Cobra flag defaults)
			volReadyTimeout, err := time.ParseDuration(volReadyTimeoutStr)
			if err != nil {
				fmt.Printf("invalid --volume-ready-timeout: %v\n", err)
				os.Exit(2)
			}
			volReadyRetryTimeout, err := time.ParseDuration(volReadyRetryTimeoutStr)
			if err != nil {
				fmt.Printf("invalid --volume-retry-timeout: %v\n", err)
				os.Exit(2)
			}

			d := driver.NewDriver(nodeID, endpoint, volReadyTimeout, volReadyRetryTimeout)
			if err := d.Run(endpoint); err != nil {
				fmt.Printf("Error running driver: %v\n", err)
				os.Exit(1)
			}
		},
	}

	cmd.Flags().StringVar(&endpoint, "endpoint", "unix:///var/lib/csi/sockets/pluginproxy/csi.sock", "CSI endpoint")
	cmd.Flags().StringVar(&nodeID, "node-id", "", "Node ID")
	cmd.Flags().BoolVarP(&debug, "debug", "d", false, "Enable debug logging")
	cmd.Flags().StringVar(&volReadyTimeoutStr, "volume-ready-timeout", "5m", "Timeout waiting for a source volume to become active (e.g. 30s, 1m)")
	cmd.Flags().StringVar(&volReadyRetryTimeoutStr, "volume-retry-timeout", "5s", "Short timeout used for quick checks before retrying clone (e.g. 5s)")

	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

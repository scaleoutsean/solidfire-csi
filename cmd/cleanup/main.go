package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	sf "github.com/scaleoutsean/solidfire-go/methods"
	"github.com/scaleoutsean/solidfire-go/sdk"
)

func main() {
	endpoint := os.Getenv("SOLIDFIRE_ENDPOINT")
	user := os.Getenv("SOLIDFIRE_USER")
	password := os.Getenv("SOLIDFIRE_PASSWORD")
	tenant := os.Getenv("SOLIDFIRE_TENANT")

	fmt.Printf("Connecting to %s as %s (Tenant: %s)\n", endpoint, user, tenant)

	client, err := sf.NewClientFromSecrets(endpoint, user, password, "11.0", tenant, "1G")
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	// List Volumes for Account
	fmt.Printf("Listing volumes for account ID %d...\n", client.AccountID)
	req := sdk.ListVolumesForAccountRequest{
		AccountID: client.AccountID,
	}

	// Use a separate variable for sdkErr to avoid nil-pointer-in-interface trap
	res, sdkErr := client.SFClient.ListVolumesForAccount(ctx, &req)
	if sdkErr != nil {
		panic(fmt.Sprintf("%s: %s", sdkErr.Code, sdkErr.Detail))
	}

	if res != nil && len(res.Volumes) > 0 {
		fmt.Printf("Found %d volumes. Processing...\n", len(res.Volumes))

		for _, v := range res.Volumes {
			fmt.Printf("Volume %d (%s) [%s]: ", v.VolumeID, v.Name, v.Status)

			if strings.ToLower(v.Status) == "active" {
				fmt.Print("Deleting... ")
				delReq := sdk.DeleteVolumeRequest{
					VolumeID: v.VolumeID,
				}
				_, delErr := client.SFClient.DeleteVolume(ctx, &delReq)
				if delErr != nil {
					fmt.Printf("Failed to delete: %v\n", delErr)
					continue
				}
				fmt.Print("Deleted. ")
			}

			// Purge
			fmt.Print("Purging... ")
			_, purgeErr := client.SFClient.PurgeDeletedVolume(ctx, &sdk.PurgeDeletedVolumeRequest{VolumeID: v.VolumeID})
			if purgeErr != nil {
				fmt.Printf("Failed to purge: %v\n", purgeErr)
			} else {
				fmt.Println("Purged.")
			}
		}
	} else {
		fmt.Println("No volumes found for this account via ListVolumesForAccount.")
	}

	// Double check explicit deleted volumes list just in case
	fmt.Println("Checking Recycled Bin...")
	delListRes, sdkErr2 := client.SFClient.ListDeletedVolumes(ctx, &sdk.ListDeletedVolumesRequest{})
	if sdkErr2 == nil && delListRes != nil {
		count := 0
		for _, v := range delListRes.Volumes {
			// Only purge if it belongs to our account
			if v.AccountID == client.AccountID {
				count++
				fmt.Printf("Purging Deleted Volume %d (%s)... ", v.VolumeID, v.Name)
				_, err := client.SFClient.PurgeDeletedVolume(ctx, &sdk.PurgeDeletedVolumeRequest{VolumeID: v.VolumeID})
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("Purged.")
				}
			}
		}
		if count == 0 {
			fmt.Println("No deleted volumes found for this account.")
		}
	}

	fmt.Println("Cleanup Complete.")
}

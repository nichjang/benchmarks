package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/cloudspannerecosystem/spanner-change-streams-tail/changestreams"
)

func main() {
	ctx := context.Background()
	reader, err := changestreams.NewReader(ctx, "cps-devel", "spanner-test", "test", "flink_resource_cdc")
	if err != nil {
		log.Fatalf("failed to create a reader: %v", err)
	}
	defer reader.Close()

	fmt.Println("connected to change stream")

	if err := reader.Read(ctx, func(result *changestreams.ReadResult) error {
		for _, cr := range result.ChangeRecords {
			for _, dcr := range cr.DataChangeRecords {
				// Calculate the difference between the two times
				diff := time.Since(dcr.CommitTimestamp)

				fmt.Printf("[%s] %s %s\n", dcr.CommitTimestamp, dcr.ModType, dcr.TableName)

				// Print the difference in seconds
				fmt.Printf("The latency is %v\n", diff)
			}
		}
		return nil
	}); err != nil {
		log.Fatalf("failed to read: %v", err)
	}
}

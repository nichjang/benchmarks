package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/cloudspannerecosystem/spanner-change-streams-tail/changestreams"
)

// LatencyRow represents a row in BigQuery.
type LatencyRow struct {
	OperationType string
	Latency       int64
	CommitTime    time.Time
}

// LatencyRow implements the ValueSaver interface.
// This example disables best-effort de-duplication, which allows for higher throughput.
func (r *LatencyRow) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"operation_type": r.OperationType,
		"latency":        r.Latency,
		"commit_ts":      r.CommitTime,
	}, bigquery.NoDedupeID, nil
}

func createBigQueryInserter(projectID, datasetID, tableID string) (*bigquery.Inserter, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %w", err)
	}
	defer client.Close()

	return client.Dataset(datasetID).Table(tableID).Inserter(), nil
}

func insertRow(ctx context.Context, inserter *bigquery.Inserter, row *LatencyRow) error {
	if err := inserter.Put(ctx, row); err != nil {
		return err
	}
	return nil
}

func main() {
	projectID := "cps-devel"
	spannerInstanceID := "spanner-test"
	spannerDatabaseID := "test"
	spannerStreamID := "flink_resource_cdc"
	bigqueryDatasetID := "spanner_benchmark"
	bigqueryTableID := "flink_resource_latency"

	ctx := context.Background()
	reader, err := changestreams.NewReader(ctx, projectID, spannerInstanceID, spannerDatabaseID, spannerStreamID)
	if err != nil {
		log.Fatalf("failed to create a reader: %v", err)
	}
	defer reader.Close()

	bqInserter, err := createBigQueryInserter(projectID, bigqueryDatasetID, bigqueryTableID)
	if err != nil {
		log.Fatalf("failed to create a bq inserter: %v", err)
	}

	fmt.Println("connected to change stream")

	if err := reader.Read(ctx, func(result *changestreams.ReadResult) error {
		for _, cr := range result.ChangeRecords {
			for _, dcr := range cr.DataChangeRecords {
				// Calculate the difference between the two times
				diff := time.Since(dcr.CommitTimestamp)

				fmt.Printf("[%s] %s %s\n", dcr.CommitTimestamp, dcr.ModType, dcr.TableName)

				// insert into bigquery
				err = insertRow(ctx, bqInserter, &LatencyRow{
					OperationType: dcr.ModType,
					Latency:       diff.Milliseconds(),
					CommitTime:    dcr.CommitTimestamp,
				})
				if err != nil {
					fmt.Printf("Err inserting into bq: %v\n", err)
				}

				// Print the difference in seconds
				fmt.Printf("The latency is %v\n", diff)
			}
		}
		return nil
	}); err != nil {
		log.Fatalf("failed to read: %v", err)
	}
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/cloudspannerecosystem/spanner-change-streams-tail/changestreams"
)

const (
	ModTypeDelete = "DELETE"
)

// LatencyRow represents a row in BigQuery.
type LatencyRow struct {
	OperationType        string
	LatencySinceCommit   int64
	LatencySinceModified int64
	CommitTime           time.Time
	ModifiedTime         time.Time
}

type PhysicalCluster struct {
	Modified time.Time `json:"modified_ts"`
}

// LatencyRow implements the ValueSaver interface.
// This example disables best-effort de-duplication, which allows for higher throughput.
func (r *LatencyRow) Save() (map[string]bigquery.Value, string, error) {
	bqValueMap := map[string]bigquery.Value{
		"operation_type":       r.OperationType,
		"latency_since_commit": r.LatencySinceCommit,
		"commit_ts":            r.CommitTime,
	}

	if r.LatencySinceModified != 0 {
		bqValueMap["latency_since_modified"] = r.LatencySinceModified
		bqValueMap["modified_ts"] = r.ModifiedTime
	}

	return bqValueMap, bigquery.NoDedupeID, nil
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
	spannerInstanceID := "spanner-benchmark"
	spannerDatabaseID := "benchmark-db"
	spannerStreamID := "pkc_benchmark_cdc"
	bigqueryDatasetID := "spanner_benchmark"
	bigqueryTableID := "pkc_latency"

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
				timeSinceCommit := time.Since(dcr.CommitTimestamp)

				latencyRow := &LatencyRow{
					OperationType:      dcr.ModType,
					LatencySinceCommit: timeSinceCommit.Milliseconds(),
					CommitTime:         dcr.CommitTimestamp,
				}

				if dcr.ModType != ModTypeDelete && len(dcr.Mods) > 0 {
					data := PhysicalCluster{}
					json.Unmarshal([]byte(dcr.Mods[0].NewValues.String()), &data)

					timeSinceModified := time.Since(data.Modified)

					// fmt.Printf("The latency from modified is %v\n", time.Since(data.Modified))
					latencyRow.LatencySinceModified = timeSinceModified.Milliseconds()
					latencyRow.ModifiedTime = data.Modified
				}

				// insert into bigquery
				err = insertRow(ctx, bqInserter, latencyRow)
				if err != nil {
					fmt.Printf("Err inserting into bq: %v\n", err)
				}
			}
		}
		return nil
	}); err != nil {
		log.Fatalf("failed to read: %v", err)
	}
}

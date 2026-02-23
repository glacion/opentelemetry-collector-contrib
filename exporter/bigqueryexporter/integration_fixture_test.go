// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/oauth2/google"
)

const runIntegrationEnv = "RUN_BIGQUERY_INTEGRATION"

type integrationFixture struct {
	ctx       context.Context
	projectID string
	datasetID string
	client    *bigquery.Client
}

func newIntegrationFixture(t *testing.T) *integrationFixture {
	t.Helper()
	if os.Getenv(runIntegrationEnv) != "1" {
		t.Skipf("skipping BigQuery integration test; set %s=1 to run", runIntegrationEnv)
	}

	ctx := t.Context()
	projectID, err := adcProjectID(ctx)
	if err != nil {
		t.Fatalf("resolve ADC project ID: %v", err)
	}

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		t.Fatalf("create BigQuery client: %v", err)
	}

	datasetID := temporaryDatasetID()
	ds := client.Dataset(datasetID)
	if err := ds.Create(ctx, &bigquery.DatasetMetadata{Location: "US"}); err != nil {
		_ = client.Close()
		t.Fatalf("create dataset %s.%s: %v", projectID, datasetID, err)
	}

	return &integrationFixture{
		ctx:       ctx,
		projectID: projectID,
		datasetID: datasetID,
		client:    client,
	}
}

func (f *integrationFixture) cleanup(t *testing.T) {
	t.Helper()
	if f.client == nil {
		return
	}

	ds := f.client.Dataset(f.datasetID)
	if err := ds.DeleteWithContents(f.ctx); err != nil {
		t.Fatalf("delete dataset %s.%s: %v", f.projectID, f.datasetID, err)
	}
	if err := f.client.Close(); err != nil {
		t.Fatalf("close BigQuery client: %v", err)
	}
}

func (f *integrationFixture) waitForRows(t *testing.T, table string, minRows int64) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		count, err := f.rowCount(table)
		if err == nil && count >= minRows {
			return
		}
		if time.Now().After(deadline) {
			if err != nil {
				t.Fatalf("wait for rows in %s failed: %v", table, err)
			}
			t.Fatalf("wait for rows in %s timed out: got %d want >= %d", table, count, minRows)
		}
		time.Sleep(2 * time.Second)
	}
}

func (f *integrationFixture) tableExists(table string) (bool, error) {
	_, err := f.client.Dataset(f.datasetID).Table(table).Metadata(f.ctx)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "not found") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (f *integrationFixture) rowCount(table string) (int64, error) {
	q := f.client.Query(fmt.Sprintf("SELECT COUNT(*) FROM `%s.%s.%s`", f.projectID, f.datasetID, table))
	it, err := q.Read(f.ctx)
	if err != nil {
		return 0, err
	}
	var values []bigquery.Value
	if err := it.Next(&values); err != nil {
		return 0, err
	}
	if len(values) != 1 {
		return 0, fmt.Errorf("unexpected row count result length: %d", len(values))
	}
	v, ok := values[0].(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected row count type: %T", values[0])
	}
	return v, nil
}

func adcProjectID(ctx context.Context) (string, error) {
	for _, k := range []string{"GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "GCP_PROJECT"} {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v, nil
		}
	}
	creds, err := google.FindDefaultCredentials(ctx, bigquery.Scope)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(creds.ProjectID) == "" {
		return "", errors.New("ADC does not expose project ID; set GOOGLE_CLOUD_PROJECT")
	}
	return creds.ProjectID, nil
}

func temporaryDatasetID() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 8)
	for i := range b {
		b[i] = chars[rand.IntN(len(chars))]
	}
	return fmt.Sprintf("oteltest_%d_%s", time.Now().Unix(), string(b))
}

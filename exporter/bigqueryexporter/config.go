// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigqueryexporter"

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const maxIdentifierLength = 1024

var bigQueryIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Config defines configuration for the BigQuery exporter.
type Config struct {
	Dataset       DatasetConfig                                            `mapstructure:"dataset"`
	TimeoutConfig exporterhelper.TimeoutConfig                             `mapstructure:",squash"`
	BackOffConfig configretry.BackOffConfig                                `mapstructure:"retry_on_failure"`
	QueueConfig   configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
}

// DatasetConfig holds BigQuery dataset and table information.
type DatasetConfig struct {
	Project string      `mapstructure:"project"`
	ID      string      `mapstructure:"id"`
	Table   TableConfig `mapstructure:",squash"`
}

// TableConfig holds the table names for each signal.
type TableConfig struct {
	Trace  string `mapstructure:"trace_table"`
	Metric string `mapstructure:"metric_table"`
	Log    string `mapstructure:"log_table"`
}

// Validate checks if the configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.Dataset.ID == "" {
		return errors.New("dataset.id is required")
	}
	if cfg.Dataset.Project != "" && strings.TrimSpace(cfg.Dataset.Project) != cfg.Dataset.Project {
		return errors.New("dataset.project must not contain leading or trailing whitespace")
	}
	if err := validateIdentifier("dataset.id", cfg.Dataset.ID); err != nil {
		return err
	}
	if err := validateIdentifier("dataset.trace_table", cfg.Dataset.Table.Trace); err != nil {
		return err
	}
	if err := validateIdentifier("dataset.metric_table", cfg.Dataset.Table.Metric); err != nil {
		return err
	}
	if err := validateIdentifier("dataset.log_table", cfg.Dataset.Table.Log); err != nil {
		return err
	}
	return nil
}

func validateIdentifier(field, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	if len(value) > maxIdentifierLength {
		return fmt.Errorf("%s length must be <= %d", field, maxIdentifierLength)
	}
	if !bigQueryIdentifierPattern.MatchString(value) {
		return fmt.Errorf("%s must match %s", field, bigQueryIdentifierPattern.String())
	}
	return nil
}

func createDefaultConfig() *Config {
	return &Config{
		BackOffConfig: configretry.NewDefaultBackOffConfig(),
		QueueConfig:   configoptional.None[exporterhelper.QueueBatchConfig](),
		Dataset: DatasetConfig{
			Table: TableConfig{
				Trace:  "trace",
				Metric: "metric",
				Log:    "log",
			},
		},
		TimeoutConfig: exporterhelper.TimeoutConfig{
			Timeout: 30 * time.Second,
		},
	}
}

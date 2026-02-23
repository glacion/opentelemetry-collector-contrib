// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigqueryexporter"

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func newStorageWriteClient(ctx context.Context, projectID string) (*managedwriter.Client, error) {
	return managedwriter.NewClient(ctx, projectID)
}

type storageAppender struct {
	stream *managedwriter.ManagedStream
	desc   protoreflect.MessageDescriptor
}

func newStorageAppender(
	ctx context.Context,
	client *managedwriter.Client,
	projectID, datasetID, tableID string,
	schema bigquery.Schema,
) (*storageAppender, error) {
	storageSchema, err := adapt.BQSchemaToStorageTableSchema(schema)
	if err != nil {
		return nil, fmt.Errorf("convert schema to storage schema: %w", err)
	}

	desc, err := adapt.StorageSchemaToProto2Descriptor(storageSchema, "root")
	if err != nil {
		return nil, fmt.Errorf("convert storage schema to descriptor: %w", err)
	}

	msgDesc, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, errors.New("adapted descriptor is not a message descriptor")
	}

	normalized, err := adapt.NormalizeDescriptor(msgDesc)
	if err != nil {
		return nil, fmt.Errorf("normalize descriptor: %w", err)
	}

	tableRef := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID)
	stream, err := client.NewManagedStream(
		ctx,
		managedwriter.WithDestinationTable(tableRef),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(normalized),
	)
	if err != nil {
		return nil, fmt.Errorf("create managed stream: %w", err)
	}
	return &storageAppender{stream: stream, desc: msgDesc}, nil
}

func appendStorageRows(ctx context.Context, appender *storageAppender, rows []map[string]bigquery.Value) error {
	serialized := make([][]byte, 0, len(rows))
	for _, row := range rows {
		b, err := encodeRow(appender.desc, row)
		if err != nil {
			return err
		}
		serialized = append(serialized, b)
	}

	result, err := appender.stream.AppendRows(ctx, serialized)
	if err != nil {
		return err
	}
	_, err = result.GetResult(ctx)
	return err
}

func encodeRow(desc protoreflect.MessageDescriptor, row map[string]bigquery.Value) ([]byte, error) {
	msg := dynamicpb.NewMessage(desc)
	fields := desc.Fields()

	for name, value := range row {
		fd := fields.ByName(protoreflect.Name(name))
		if fd == nil || value == nil {
			continue
		}
		if err := setFieldValue(msg, fd, value); err != nil {
			return nil, fmt.Errorf("set field %q: %w", name, err)
		}
	}

	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal row: %w", err)
	}
	return b, nil
}

func setFieldValue(msg *dynamicpb.Message, fd protoreflect.FieldDescriptor, value bigquery.Value) error {
	switch fd.Kind() {
	case protoreflect.MessageKind:
		wrapped, err := dynamicWrapperValue(fd.Message(), value)
		if err != nil {
			return err
		}
		msg.Set(fd, wrapped)
	default:
		v, err := toProtoreflectValue(fd.Kind(), value)
		if err != nil {
			return err
		}
		msg.Set(fd, v)
	}
	return nil
}

func dynamicWrapperValue(desc protoreflect.MessageDescriptor, value bigquery.Value) (protoreflect.Value, error) {
	field := desc.Fields().ByName(protoreflect.Name("value"))
	if field == nil {
		return protoreflect.Value{}, fmt.Errorf("unsupported message type %s", desc.FullName())
	}

	wrapped := dynamicpb.NewMessage(desc)
	v, err := toProtoreflectValue(field.Kind(), value)
	if err != nil {
		return protoreflect.Value{}, fmt.Errorf("wrapper value for %s: %w", desc.FullName(), err)
	}
	wrapped.Set(field, v)

	return protoreflect.ValueOfMessage(wrapped.ProtoReflect()), nil
}

func toProtoreflectValue(kind protoreflect.Kind, value any) (protoreflect.Value, error) {
	switch kind {
	case protoreflect.StringKind:
		s, err := asString(value)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfString(s), nil
	case protoreflect.BoolKind:
		b, err := asBool(value)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfBool(b), nil
	case protoreflect.Int64Kind:
		i, err := asInt64(value)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt64(i), nil
	case protoreflect.DoubleKind:
		d, err := asFloat64(value)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfFloat64(d), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported field kind %v", kind)
	}
}

func asString(value any) (string, error) {
	s, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("expected string, got %T", value)
	}
	return s, nil
}

func asBool(value any) (bool, error) {
	b, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("expected bool, got %T", value)
	}
	return b, nil
}

func asInt64(value any) (int64, error) {
	switch n := value.(type) {
	case int:
		return int64(n), nil
	case int64:
		return n, nil
	case uint64:
		return int64(n), nil
	case uint32:
		return int64(n), nil
	case float64:
		return int64(n), nil
	case time.Time:
		return n.UnixMicro(), nil
	default:
		return 0, fmt.Errorf("expected int64-compatible value, got %T", value)
	}
}

func asFloat64(value any) (float64, error) {
	switch n := value.(type) {
	case float64:
		return n, nil
	case float32:
		return float64(n), nil
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	default:
		return 0, fmt.Errorf("expected float64-compatible value, got %T", value)
	}
}

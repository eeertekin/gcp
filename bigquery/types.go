package bigquery

import (
	"fmt"
	"time"
)

type FieldType string

const (
	StringFieldType     FieldType = "STRING"
	BytesFieldType      FieldType = "BYTES"
	IntegerFieldType    FieldType = "INTEGER"
	FloatFieldType      FieldType = "FLOAT"
	BooleanFieldType    FieldType = "BOOLEAN"
	TimestampFieldType  FieldType = "TIMESTAMP"
	RecordFieldType     FieldType = "RECORD"
	DateFieldType       FieldType = "DATE"
	TimeFieldType       FieldType = "TIME"
	DateTimeFieldType   FieldType = "DATETIME"
	NumericFieldType    FieldType = "NUMERIC"
	BigNumericFieldType FieldType = "BIGNUMERIC"
	GeographyFieldType  FieldType = "GEOGRAPHY"
	IntervalFieldType   FieldType = "INTERVAL"
	JSONFieldType       FieldType = "JSON"
	RangeFieldType      FieldType = "RANGE"
)

type DataFormat string

const (
	CSV     DataFormat = "CSV"
	Avro    DataFormat = "AVRO"
	JSON    DataFormat = "NEWLINE_DELIMITED_JSON"
	Parquet DataFormat = "PARQUET"
	ORC     DataFormat = "ORC"
)

type TableCreateDisposition string

const (
	CreateIfNeeded TableCreateDisposition = "CREATE_IF_NEEDED"
	CreateNever    TableCreateDisposition = "CREATE_NEVER"
)

type TableWriteDisposition string

const (
	WriteAppend   TableWriteDisposition = "WRITE_APPEND"
	WriteTruncate TableWriteDisposition = "WRITE_TRUNCATE"
	WriteEmpty    TableWriteDisposition = "WRITE_EMPTY"
)

type FieldSchema struct {
	Name        string
	Description string
	Repeated    bool
	Required    bool
	Type        FieldType
	Schema      Schema
}

type Schema []*FieldSchema

type TimePartitioningType string

const (
	DayPartitioningType   TimePartitioningType = "DAY"
	HourPartitioningType  TimePartitioningType = "HOUR"
	MonthPartitioningType TimePartitioningType = "MONTH"
	YearPartitioningType  TimePartitioningType = "YEAR"
)

type TimePartitioning struct {
	Type                   TimePartitioningType
	Expiration             time.Duration
	Field                  string
	RequirePartitionFilter bool
}

type TableMetadata struct {
	Description      string
	Schema           Schema
	TimePartitioning *TimePartitioning
}

type State int

const (
	StateUnspecified State = iota
	Pending
	Running
	Done
)

type JobStatus struct {
	State  State
	Errors []*Error

	err error
}

func (s *JobStatus) Done() bool {
	return s.State == Done
}

func (s *JobStatus) Err() error {
	return s.err
}

type Error struct {
	Location string
	Message  string
	Reason   string
}

func (e Error) Error() string {
	return fmt.Sprintf("{Location: %q; Message: %q; Reason: %q}", e.Location, e.Message, e.Reason)
}

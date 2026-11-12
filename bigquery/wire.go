package bigquery

import "time"

type tableReference struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
	TableID   string `json:"tableId"`
}

type wireField struct {
	Name        string      `json:"name"`
	Type        FieldType   `json:"type"`
	Mode        string      `json:"mode,omitempty"`
	Description string      `json:"description,omitempty"`
	Fields      []wireField `json:"fields,omitempty"`
}

type wireSchema struct {
	Fields []wireField `json:"fields"`
}

type wireTimePartitioning struct {
	Type                   string `json:"type"`
	ExpirationMs           int64  `json:"expirationMs,string,omitempty"`
	Field                  string `json:"field,omitempty"`
	RequirePartitionFilter bool   `json:"requirePartitionFilter,omitempty"`
}

type tableResource struct {
	TableReference   tableReference        `json:"tableReference"`
	Description      string                `json:"description,omitempty"`
	Schema           *wireSchema           `json:"schema,omitempty"`
	TimePartitioning *wireTimePartitioning `json:"timePartitioning,omitempty"`
}

type jobReference struct {
	ProjectID string `json:"projectId"`
	JobID     string `json:"jobId"`
	Location  string `json:"location,omitempty"`
}

type jobConfigurationLoad struct {
	SourceURIs          []string               `json:"sourceUris"`
	SourceFormat        DataFormat             `json:"sourceFormat,omitempty"`
	DestinationTable    tableReference         `json:"destinationTable"`
	Schema              *wireSchema            `json:"schema,omitempty"`
	Autodetect          bool                   `json:"autodetect,omitempty"`
	IgnoreUnknownValues bool                   `json:"ignoreUnknownValues,omitempty"`
	AllowJaggedRows     bool                   `json:"allowJaggedRows,omitempty"`
	MaxBadRecords       int64                  `json:"maxBadRecords,omitempty"`
	WriteDisposition    TableWriteDisposition  `json:"writeDisposition,omitempty"`
	CreateDisposition   TableCreateDisposition `json:"createDisposition,omitempty"`
	TimePartitioning    *wireTimePartitioning  `json:"timePartitioning,omitempty"`
}

type jobConfigurationTableCopy struct {
	SourceTables      []tableReference       `json:"sourceTables"`
	DestinationTable  tableReference         `json:"destinationTable"`
	WriteDisposition  TableWriteDisposition  `json:"writeDisposition,omitempty"`
	CreateDisposition TableCreateDisposition `json:"createDisposition,omitempty"`
}

type jobConfiguration struct {
	Load *jobConfigurationLoad      `json:"load,omitempty"`
	Copy *jobConfigurationTableCopy `json:"copy,omitempty"`
}

type errorProto struct {
	Reason   string `json:"reason"`
	Location string `json:"location"`
	Message  string `json:"message"`
}

type jobStatusResource struct {
	State       string       `json:"state"`
	ErrorResult *errorProto  `json:"errorResult,omitempty"`
	Errors      []errorProto `json:"errors,omitempty"`
}

type jobResource struct {
	JobReference  jobReference       `json:"jobReference"`
	Configuration jobConfiguration   `json:"configuration"`
	Status        *jobStatusResource `json:"status,omitempty"`
}

func (s Schema) toWire() []wireField {
	out := make([]wireField, 0, len(s))
	for _, f := range s {
		wf := wireField{
			Name:        f.Name,
			Type:        f.Type,
			Description: f.Description,
			Fields:      f.Schema.toWire(),
		}
		switch {
		case f.Repeated:
			wf.Mode = "REPEATED"
		case f.Required:
			wf.Mode = "REQUIRED"
		}
		out = append(out, wf)
	}
	return out
}

func schemaFromWire(fs []wireField) Schema {
	if len(fs) == 0 {
		return nil
	}
	out := make(Schema, 0, len(fs))
	for _, f := range fs {
		out = append(out, &FieldSchema{
			Name:        f.Name,
			Description: f.Description,
			Type:        f.Type,
			Repeated:    f.Mode == "REPEATED",
			Required:    f.Mode == "REQUIRED",
			Schema:      schemaFromWire(f.Fields),
		})
	}
	return out
}

func (p *TimePartitioning) toWire() *wireTimePartitioning {
	if p == nil {
		return nil
	}
	typ := p.Type
	if typ == "" {
		typ = DayPartitioningType
	}
	return &wireTimePartitioning{
		Type:                   string(typ),
		ExpirationMs:           p.Expiration.Milliseconds(),
		Field:                  p.Field,
		RequirePartitionFilter: p.RequirePartitionFilter,
	}
}

func timePartitioningFromWire(w *wireTimePartitioning) *TimePartitioning {
	if w == nil {
		return nil
	}
	return &TimePartitioning{
		Type:                   TimePartitioningType(w.Type),
		Expiration:             time.Duration(w.ExpirationMs) * time.Millisecond,
		Field:                  w.Field,
		RequirePartitionFilter: w.RequirePartitionFilter,
	}
}

func statusFromResource(r *jobStatusResource) *JobStatus {
	s := &JobStatus{}
	if r == nil {
		return s
	}
	switch r.State {
	case "PENDING":
		s.State = Pending
	case "RUNNING":
		s.State = Running
	case "DONE":
		s.State = Done
	}
	if r.ErrorResult != nil {
		s.err = &Error{Location: r.ErrorResult.Location, Message: r.ErrorResult.Message, Reason: r.ErrorResult.Reason}
	}
	for _, e := range r.Errors {
		s.Errors = append(s.Errors, &Error{Location: e.Location, Message: e.Message, Reason: e.Reason})
	}
	return s
}

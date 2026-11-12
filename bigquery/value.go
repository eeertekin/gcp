package bigquery

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/eeertekin/gcp/civil"
)

type Value any

const (
	numericScaleDigits    = 9
	bigNumericScaleDigits = 38
)

func NumericString(r *big.Rat) string {
	return r.FloatString(numericScaleDigits)
}

func BigNumericString(r *big.Rat) string {
	return r.FloatString(bigNumericScaleDigits)
}

type RangeValue struct {
	Start Value
	End   Value
}

type IntervalValue struct {
	Years          int32
	Months         int32
	Days           int32
	Hours          int32
	Minutes        int32
	Seconds        int32
	SubSecondNanos int32
}

func abs32(v int32) int32 {
	if v < 0 {
		return -v
	}
	return v
}

func (iv *IntervalValue) String() string {
	ym := fmt.Sprintf("%d-%d", abs32(iv.Years), abs32(iv.Months))
	if iv.Years < 0 || iv.Months < 0 {
		ym = "-" + ym
	}
	hms := fmt.Sprintf("%d:%d:%d", abs32(iv.Hours), abs32(iv.Minutes), abs32(iv.Seconds))
	if iv.SubSecondNanos != 0 {
		hms += "." + strings.TrimRight(fmt.Sprintf("%09d", abs32(iv.SubSecondNanos)), "0")
	}
	if iv.Hours < 0 || iv.Minutes < 0 || iv.Seconds < 0 || iv.SubSecondNanos < 0 {
		hms = "-" + hms
	}
	return fmt.Sprintf("%s %d %s", ym, iv.Days, hms)
}

func parseInterval(value string) (*IntervalValue, error) {
	bad := fmt.Errorf("bigquery: invalid INTERVAL %q", value)
	parts := strings.Fields(value)
	if len(parts) != 3 {
		return nil, bad
	}
	num := func(s string) (int32, bool) {
		n, err := strconv.ParseInt(s, 10, 32)
		return int32(n), err == nil
	}
	sign := func(s string) (bool, string) {
		if strings.HasPrefix(s, "-") {
			return true, s[1:]
		}
		return false, strings.TrimPrefix(s, "+")
	}
	iv := &IntervalValue{}
	var ok bool

	neg, ym := sign(parts[0])
	y, m, found := strings.Cut(ym, "-")
	if !found {
		return nil, bad
	}
	if iv.Years, ok = num(y); !ok {
		return nil, bad
	}
	if iv.Months, ok = num(m); !ok {
		return nil, bad
	}
	if neg {
		iv.Years, iv.Months = -iv.Years, -iv.Months
	}

	if iv.Days, ok = num(parts[1]); !ok {
		return nil, bad
	}

	neg, hms := sign(parts[2])
	hm := strings.SplitN(hms, ":", 3)
	if len(hm) != 3 {
		return nil, bad
	}
	if iv.Hours, ok = num(hm[0]); !ok {
		return nil, bad
	}
	if iv.Minutes, ok = num(hm[1]); !ok {
		return nil, bad
	}
	sec, frac, _ := strings.Cut(hm[2], ".")
	if iv.Seconds, ok = num(sec); !ok {
		return nil, bad
	}
	if frac != "" {
		if len(frac) > 9 {
			return nil, bad
		}
		if iv.SubSecondNanos, ok = num(frac + strings.Repeat("0", 9-len(frac))); !ok {
			return nil, bad
		}
	}
	if neg {
		iv.Hours, iv.Minutes, iv.Seconds, iv.SubSecondNanos = -iv.Hours, -iv.Minutes, -iv.Seconds, -iv.SubSecondNanos
	}
	return iv, nil
}

type wireCell struct {
	V json.RawMessage `json:"v"`
}

type wireRow struct {
	F []wireCell `json:"f"`
}

func decodeRow(row wireRow, schema Schema) ([]Value, error) {
	if len(row.F) != len(schema) {
		return nil, fmt.Errorf("bigquery: row has %d cells, schema has %d fields", len(row.F), len(schema))
	}
	vals := make([]Value, len(row.F))
	for i, c := range row.F {
		v, err := decodeValue(c.V, schema[i])
		if err != nil {
			return nil, fmt.Errorf("bigquery: field %s: %w", schema[i].Name, err)
		}
		vals[i] = v
	}
	return vals, nil
}

func decodeValue(raw json.RawMessage, f *FieldSchema) (Value, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	if f.Repeated {
		var cells []wireCell
		if err := json.Unmarshal(raw, &cells); err != nil {
			return nil, err
		}
		elem := *f
		elem.Repeated = false
		vals := make([]Value, len(cells))
		for i, c := range cells {
			v, err := decodeValue(c.V, &elem)
			if err != nil {
				return nil, err
			}
			vals[i] = v
		}
		return vals, nil
	}
	if canonical(f.Type) == RecordFieldType {
		var row wireRow
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, err
		}
		return decodeRow(row, f.Schema)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	return parseScalar(s, f)
}

func canonical(t FieldType) FieldType {
	switch t {
	case "INT64":
		return IntegerFieldType
	case "FLOAT64":
		return FloatFieldType
	case "BOOL":
		return BooleanFieldType
	case "STRUCT":
		return RecordFieldType
	}
	return t
}

func parseScalar(s string, f *FieldSchema) (Value, error) {
	switch canonical(f.Type) {
	case IntegerFieldType:
		return strconv.ParseInt(s, 10, 64)
	case FloatFieldType:
		return strconv.ParseFloat(s, 64)
	case BooleanFieldType:
		return strconv.ParseBool(s)
	case BytesFieldType:
		return base64.StdEncoding.DecodeString(s)
	case NumericFieldType, BigNumericFieldType:
		r, ok := new(big.Rat).SetString(s)
		if !ok {
			return nil, fmt.Errorf("invalid %s %q", f.Type, s)
		}
		return r, nil
	case DateFieldType:
		return civil.ParseDate(s)
	case TimeFieldType:
		return civil.ParseTime(s)
	case DateTimeFieldType:
		return civil.ParseDateTime(s)
	case TimestampFieldType:
		return parseTimestamp(s)
	case IntervalFieldType:
		return parseInterval(s)
	case RangeFieldType:
		return parseRange(s, f)
	}
	return s, nil
}

// Requests ask for formatOptions.useInt64Timestamp, so TIMESTAMP cells arrive
// as integer microseconds; float seconds and text are older wire forms.
func parseTimestamp(s string) (time.Time, error) {
	if us, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.UnixMicro(us).UTC(), nil
	}
	secs, err := strconv.ParseFloat(s, 64)
	if err != nil {
		if t, perr := time.Parse("2006-01-02 15:04:05.999999999-07", s); perr == nil {
			return t.UTC(), nil
		}
		return time.Time{}, fmt.Errorf("invalid TIMESTAMP %q", s)
	}
	return time.UnixMicro(int64(math.Round(secs * 1e6))).UTC(), nil
}

func parseRange(s string, f *FieldSchema) (*RangeValue, error) {
	if len(s) < 2 || s[0] != '[' || s[len(s)-1] != ')' {
		return nil, fmt.Errorf("invalid RANGE %q", s)
	}
	start, end, ok := strings.Cut(s[1:len(s)-1], ",")
	if !ok {
		return nil, fmt.Errorf("invalid RANGE %q", s)
	}
	elem := &FieldSchema{Type: StringFieldType}
	if f.RangeElementType != nil {
		elem.Type = f.RangeElementType.Type
	}
	bound := func(b string) (Value, error) {
		b = strings.TrimSpace(b)
		if b == "UNBOUNDED" || b == "NULL" {
			return nil, nil
		}
		return parseScalar(b, elem)
	}
	rv := &RangeValue{}
	var err error
	if rv.Start, err = bound(start); err != nil {
		return nil, err
	}
	if rv.End, err = bound(end); err != nil {
		return nil, err
	}
	return rv, nil
}

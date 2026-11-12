package civil

import (
	"testing"
	"time"
)

func TestParseAndString(t *testing.T) {
	d, err := ParseDate("2026-02-03")
	if err != nil || d != (Date{2026, time.February, 3}) || d.String() != "2026-02-03" {
		t.Fatalf("date %v %v", d, err)
	}
	tm, err := ParseTime("04:05:06.000007")
	if err != nil || tm != (Time{4, 5, 6, 7000}) || tm.String() != "04:05:06.000007000" {
		t.Fatalf("time %v %v", tm, err)
	}
	if s := (Time{1, 2, 3, 0}).String(); s != "01:02:03" {
		t.Fatalf("time string %q", s)
	}
	for _, in := range []string{"2026-02-03T04:05:06.5", "2026-02-03t04:05:06.5", "2026-02-03 04:05:06.5"} {
		dt, err := ParseDateTime(in)
		if err != nil || dt != (DateTime{Date{2026, 2, 3}, Time{4, 5, 6, 500000000}}) {
			t.Fatalf("%q: %v %v", in, dt, err)
		}
	}
	if s := (DateTime{Date{2026, 2, 3}, Time{4, 5, 6, 0}}).String(); s != "2026-02-03T04:05:06" {
		t.Fatalf("datetime string %q", s)
	}
	for _, bad := range []string{"2026-13-01", "2026-02-30", "x"} {
		if _, err := ParseDate(bad); err == nil {
			t.Errorf("ParseDate(%q) should fail", bad)
		}
	}
	if _, err := ParseTime("25:00:00"); err == nil {
		t.Error("ParseTime(25:00:00) should fail")
	}
	if !(Date{2024, 2, 29}).IsValid() || (Date{2026, 2, 29}).IsValid() {
		t.Error("IsValid wrong for leap days")
	}
	if got := (DateTime{Date{2026, 1, 2}, Time{3, 4, 5, 6}}).In(time.UTC); !got.Equal(time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)) {
		t.Errorf("DateTime.In = %v", got)
	}
}

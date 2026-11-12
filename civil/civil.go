package civil

import (
	"fmt"
	"strings"
	"time"
)

type Date struct {
	Year  int
	Month time.Month
	Day   int
}

func DateOf(t time.Time) Date {
	var d Date
	d.Year, d.Month, d.Day = t.Date()
	return d
}

func ParseDate(s string) (Date, error) {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return Date{}, err
	}
	return DateOf(t), nil
}

func (d Date) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", d.Year, d.Month, d.Day)
}

func (d Date) IsValid() bool {
	return DateOf(d.In(time.UTC)) == d
}

func (d Date) In(loc *time.Location) time.Time {
	return time.Date(d.Year, d.Month, d.Day, 0, 0, 0, 0, loc)
}

type Time struct {
	Hour       int
	Minute     int
	Second     int
	Nanosecond int
}

func TimeOf(t time.Time) Time {
	var tm Time
	tm.Hour, tm.Minute, tm.Second = t.Clock()
	tm.Nanosecond = t.Nanosecond()
	return tm
}

func ParseTime(s string) (Time, error) {
	t, err := time.Parse("15:04:05.999999999", s)
	if err != nil {
		return Time{}, err
	}
	return TimeOf(t), nil
}

func (t Time) String() string {
	s := fmt.Sprintf("%02d:%02d:%02d", t.Hour, t.Minute, t.Second)
	if t.Nanosecond == 0 {
		return s
	}
	return s + fmt.Sprintf(".%09d", t.Nanosecond)
}

func (t Time) IsValid() bool {
	return t.Hour >= 0 && t.Hour < 24 && t.Minute >= 0 && t.Minute < 60 &&
		t.Second >= 0 && t.Second < 60 && t.Nanosecond >= 0 && t.Nanosecond < 1e9
}

type DateTime struct {
	Date Date
	Time Time
}

func DateTimeOf(t time.Time) DateTime {
	return DateTime{Date: DateOf(t), Time: TimeOf(t)}
}

func ParseDateTime(s string) (DateTime, error) {
	if strings.IndexAny(s, "tT ") == 10 {
		s = s[:10] + "T" + s[11:]
	}
	t, err := time.Parse("2006-01-02T15:04:05.999999999", s)
	if err != nil {
		return DateTime{}, err
	}
	return DateTimeOf(t), nil
}

func (dt DateTime) String() string {
	return dt.Date.String() + "T" + dt.Time.String()
}

func (dt DateTime) IsValid() bool {
	return dt.Date.IsValid() && dt.Time.IsValid()
}

func (dt DateTime) In(loc *time.Location) time.Time {
	return time.Date(dt.Date.Year, dt.Date.Month, dt.Date.Day, dt.Time.Hour, dt.Time.Minute, dt.Time.Second, dt.Time.Nanosecond, loc)
}

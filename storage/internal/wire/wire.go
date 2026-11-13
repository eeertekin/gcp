package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

const (
	Varint  = 0
	Fixed64 = 1
	Bytes   = 2
	Fixed32 = 5
)

type Buf []byte

func (b Buf) Tag(field, wire int) Buf {
	return binary.AppendUvarint(b, uint64(field)<<3|uint64(wire))
}

func (b Buf) Uint(field int, v uint64) Buf {
	return binary.AppendUvarint(b.Tag(field, Varint), v)
}

func (b Buf) Int(field int, v int64) Buf {
	if v == 0 {
		return b
	}
	return b.Uint(field, uint64(v))
}

func (b Buf) OptInt(field int, v int64) Buf {
	return b.Uint(field, uint64(v))
}

func (b Buf) Bool(field int, v bool) Buf {
	if !v {
		return b
	}
	return b.Uint(field, 1)
}

func (b Buf) Fixed32(field int, v uint32) Buf {
	return binary.LittleEndian.AppendUint32(b.Tag(field, Fixed32), v)
}

func (b Buf) Bytes(field int, v []byte) Buf {
	b = binary.AppendUvarint(b.Tag(field, Bytes), uint64(len(v)))
	return append(b, v...)
}

func (b Buf) Str(field int, v string) Buf {
	if v == "" {
		return b
	}
	return b.Bytes(field, []byte(v))
}

func (b Buf) Msg(field int, m Buf) Buf {
	return b.Bytes(field, m)
}

type Field struct {
	Num  int
	Wire int
	U    uint64
	Data []byte
}

var ErrMalformed = errors.New("malformed protobuf message")

func Fields(b []byte, fn func(f Field) error) error {
	for len(b) > 0 {
		key, n := binary.Uvarint(b)
		if n <= 0 {
			return ErrMalformed
		}
		b = b[n:]
		f := Field{Num: int(key >> 3), Wire: int(key & 7)}
		switch f.Wire {
		case Varint:
			f.U, n = binary.Uvarint(b)
			if n <= 0 {
				return ErrMalformed
			}
			b = b[n:]
		case Fixed64:
			if len(b) < 8 {
				return ErrMalformed
			}
			f.U, b = binary.LittleEndian.Uint64(b), b[8:]
		case Fixed32:
			if len(b) < 4 {
				return ErrMalformed
			}
			f.U, b = uint64(binary.LittleEndian.Uint32(b)), b[4:]
		case Bytes:
			l, n := binary.Uvarint(b)
			if n <= 0 || uint64(len(b)-n) < l {
				return ErrMalformed
			}
			f.Data, b = b[n:n+int(l)], b[n+int(l):]
		default:
			return fmt.Errorf("unsupported protobuf wire type %d", f.Wire)
		}
		if err := fn(f); err != nil {
			return err
		}
	}
	return nil
}

func Timestamp(t time.Time) Buf {
	if t.IsZero() {
		return nil
	}
	return Buf(nil).Int(1, t.Unix()).Int(2, int64(t.Nanosecond()))
}

func ParseTimestamp(b []byte) (time.Time, error) {
	var sec, nsec int64
	err := Fields(b, func(f Field) error {
		switch f.Num {
		case 1:
			sec = int64(f.U)
		case 2:
			nsec = int64(f.U)
		}
		return nil
	})
	return time.Unix(sec, nsec).UTC(), err
}

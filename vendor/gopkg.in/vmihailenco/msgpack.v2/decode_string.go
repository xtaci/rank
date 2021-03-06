package msgpack

import (
	"fmt"
	"io"
	"reflect"

	"gopkg.in/vmihailenco/msgpack.v2/codes"
)

func (d *Decoder) DecodeString() (string, error) {
	c, err := d.r.ReadByte()
	if err != nil {
		return "", err
	}
	return d.string(c)
}

func (d *Decoder) string(c byte) (string, error) {
	n, err := d.bytesLen(c)
	if err != nil {
		return "", err
	}
	if n == -1 {
		return "", nil
	}
	b, err := d.readN(n)
	return string(b), err
}

func decodeStringValue(d *Decoder, v reflect.Value) error {
	s, err := d.DecodeString()
	if err != nil {
		return err
	}
	v.SetString(s)
	return nil
}

func (d *Decoder) DecodeBytesLen() (int, error) {
	c, err := d.r.ReadByte()
	if err != nil {
		return 0, err
	}
	return d.bytesLen(c)
}

func (d *Decoder) bytesLen(c byte) (int, error) {
	if c == codes.Nil {
		return -1, nil
	} else if codes.IsFixedString(c) {
		return int(c & codes.FixedStrMask), nil
	}
	switch c {
	case codes.Str8, codes.Bin8:
		n, err := d.uint8()
		return int(n), err
	case codes.Str16, codes.Bin16:
		n, err := d.uint16()
		return int(n), err
	case codes.Str32, codes.Bin32:
		n, err := d.uint32()
		return int(n), err
	}
	return 0, fmt.Errorf("msgpack: invalid code %x decoding bytes length", c)
}

func (d *Decoder) DecodeBytes() ([]byte, error) {
	c, err := d.r.ReadByte()
	if err != nil {
		return nil, err
	}
	return d.bytes(c, nil)
}

func (d *Decoder) bytes(c byte, b []byte) ([]byte, error) {
	n, err := d.bytesLen(c)
	if err != nil {
		return nil, err
	}
	if n == -1 {
		return nil, nil
	}

	if b == nil {
		b = make([]byte, n)
	} else if len(b) != n {
		b = setBytesLen(b, n)
	}

	_, err = io.ReadFull(d.r, b)
	return b, err
}

func (d *Decoder) bytesNoCopy() ([]byte, error) {
	c, err := d.r.ReadByte()
	if err != nil {
		return nil, err
	}
	n, err := d.bytesLen(c)
	if err != nil {
		return nil, err
	}
	if n == -1 {
		return nil, nil
	}
	return d.readN(n)
}

func (d *Decoder) decodeBytesPtr(ptr *[]byte) error {
	c, err := d.r.ReadByte()
	if err != nil {
		return err
	}
	return d.bytesPtr(c, ptr)
}

func (d *Decoder) bytesPtr(c byte, ptr *[]byte) error {
	n, err := d.bytesLen(c)
	if err != nil {
		return err
	}
	if n == -1 {
		*ptr = nil
		return nil
	}

	b := *ptr
	if b == nil {
		*ptr = make([]byte, n)
		b = *ptr
	} else if len(b) != n {
		*ptr = setBytesLen(b, n)
		b = *ptr
	}

	_, err = io.ReadFull(d.r, b)
	return err
}

func (d *Decoder) skipBytes(c byte) error {
	n, err := d.bytesLen(c)
	if err != nil {
		return err
	}
	if n == -1 {
		return nil
	}
	return d.skipN(n)
}

func decodeBytesValue(d *Decoder, v reflect.Value) error {
	c, err := d.r.ReadByte()
	if err != nil {
		return err
	}

	b, err := d.bytes(c, v.Bytes())
	if err != nil {
		return err
	}
	v.SetBytes(b)

	return nil
}

func decodeByteArrayValue(d *Decoder, v reflect.Value) error {
	c, err := d.r.ReadByte()
	if err != nil {
		return err
	}

	n, err := d.bytesLen(c)
	if err != nil {
		return err
	}
	if n == -1 {
		return nil
	}
	if n > v.Cap() {
		n = v.Cap()
	}

	b := v.Slice(0, n).Bytes()
	_, err = io.ReadFull(d.r, b)
	return err
}

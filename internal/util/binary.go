package util

import (
	"encoding/binary"
	"io"
)

func WriteString(val string, w io.Writer) error {
	return WriteBytes([]byte(val), w)
}

func WriteInt16(val int16, w io.Writer) error {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(val))
	return WriteBytes(b, w)
}

func WriteInt32(val int32, w io.Writer) error {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(val))
	return WriteBytes(b, w)
}

func WriteInt64(val int64, w io.Writer) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(val))
	return WriteBytes(b, w)
}

func WriteUint(number any, w io.Writer) error {
	return binary.Write(w, binary.BigEndian, number)
}

func WriteBytes(bytes []byte, w io.Writer) error {
	_, err := w.Write(bytes)
	return err
}

func ReadString(length int, r io.Reader) (string, error) {
	bytes, err := ReadBytes(length, r)
	return string(bytes), err
}

func ReadInt16(r io.Reader) (int16, error) {
	b, err := ReadBytes(2, r)
	return int16(binary.BigEndian.Uint16(b)), err
}

func ReadInt32(r io.Reader) (int32, error) {
	b, err := ReadBytes(4, r)
	return int32(binary.BigEndian.Uint32(b)), err
}

func ReadInt64(r io.Reader) (int64, error) {
	b, err := ReadBytes(8, r)
	return int64(binary.BigEndian.Uint64(b)), err
}

func ReadUint16(r io.Reader) (uint16, error) {
	bytes, err := ReadBytes(2, r)
	return binary.BigEndian.Uint16(bytes), err
}

func ReadUint32(r io.Reader) (uint32, error) {
	bytes, err := ReadBytes(4, r)
	return binary.BigEndian.Uint32(bytes), err
}

func ReadUint64(r io.Reader) (uint64, error) {
	bytes, err := ReadBytes(8, r)
	return binary.BigEndian.Uint64(bytes), err
}

func ReadBytes(length int, r io.Reader) ([]byte, error) {
	buff := make([]byte, length)
	_, err := r.Read(buff)
	return buff, err
}

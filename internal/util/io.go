package util

import (
	"encoding/binary"
	"io"
)

func WriteString(val string, w io.Writer) error {
	return WriteBytes([]byte(val), w)
}

func WriteNumber[T any](val T, w io.Writer) error {
	return binary.Write(w, binary.BigEndian, val)
}

func WriteBytes(bytes []byte, w io.Writer) error {
	_, err := w.Write(bytes)
	return err
}

func ReadString(length int, r io.Reader) (string, error) {
	bytes, err := ReadBytes(length, r)
	return string(bytes), err
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

package summary

import (
	"os"
)

type Summary struct {
	pageLen int
	keys    []string
	offsets []int
}

func DeserializeHeader(f *os.File, len int) (string, string) {

	return "", ""
}

func Deserialize(f *os.File, len int) *Summary {
	return nil
}

func (sum *Summary) Serialize(f *os.File) int {

	return 0
}

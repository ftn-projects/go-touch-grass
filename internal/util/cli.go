package util

import (
	"bufio"
	"fmt"
	"strings"
)

func ScanString(sc *bufio.Scanner) string {
	sc.Scan()
	return strings.TrimSpace(sc.Text())
}

func ScanLowerString(sc *bufio.Scanner) string {
	return strings.ToLower(ScanString(sc))
}

func Print(values ...string) {
	for _, v := range values {
		fmt.Print(v)
	}
	fmt.Println()
}

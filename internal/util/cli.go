package util

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

func ScanString(sc *bufio.Scanner) string {
	sc.Scan()
	return strings.TrimSpace(sc.Text())
}

func ScanLowerString(sc *bufio.Scanner) string {
	return strings.ToLower(ScanString(sc))
}

func ScanInt(sc *bufio.Scanner) int {
	i, err := strconv.Atoi(ScanString(sc))
	if err != nil || i <= 0 {
		return -1
	}
	return i
}

func Print(values ...string) {
	for _, v := range values {
		fmt.Print(v)
	}
	fmt.Println()
}

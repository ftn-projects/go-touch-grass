package menu

import (
	"fmt"
	"go-touch-grass/internal/app"
	"os"
	"strings"
)

func LoadTestData() {
	app, err := app.New()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	keys, values := getData()

	app.UnlockTokenBucket()
	for i := 0; i < len(keys); i++ {
		app.Put(keys[i], values[i])
	}
}

func getData() (keys []string, values [][]byte) {
	buf, _ := os.ReadFile("menu/test_data.csv")
	data := string(buf)

	for _, l := range strings.Split(data, "\n") {
		l := strings.TrimSpace(l)
		if l == "" {
			continue
		}

		tokens := strings.Split(l, ",")
		keys = append(keys, tokens[0])
		values = append(values, []byte(tokens[1]))
	}
	return
}

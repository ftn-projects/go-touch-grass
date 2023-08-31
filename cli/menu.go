package cli

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
)

func MainMenu() {
	var i int
	var err error
	input := bufio.NewScanner(os.Stdin)
	for {
		PrintMenu()
		for {
			fmt.Print("Izaberite opciju: ")
			input.Scan()
			i, err = strconv.Atoi(input.Text())
			if err != nil || i < 1 || i > 5 {
				fmt.Println("Niste uneli validnu opciju")
				continue
			}
			break
		}

		switch i {
		case 1:
			fmt.Println("funkcija nije implementirana")
		case 2:
			fmt.Println("funkcija nije implementirana")
		case 3:
			fmt.Println("funkcija nije implementirana")
		case 4:
			fmt.Println("funkcija nije implementirana")
		case 5:
			return
		}
	}
}

func PrintMenu() {
	fmt.Println("---------------------")
	fmt.Println("1 Upisi podatak")
	fmt.Println("2 Pronadji podatak")
	fmt.Println("3 Obrisi podatak")
	fmt.Println("4 Pokreni kompakciju")
	fmt.Println("5 Izadji")
	fmt.Println("---------------------")
}

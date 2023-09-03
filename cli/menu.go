package cli

import (
	"bufio"
	"fmt"
	application "go-touch-grass/internal/app"
	"os"
	"strings"
)

type MainMenu struct {
	app *application.App
}

func NewMenu(app *application.App) *MainMenu {
	return &MainMenu{app}
}

func (m *MainMenu) Show() {
	sc := bufio.NewScanner(os.Stdin)
	for {
		m.PrintMenu()
		fmt.Print("Izaberite opciju: ")
		sc.Scan()

		c := strings.TrimSpace(sc.Text())
		switch c {
		case "1":
			m.HandlePut(sc)
		case "2":
			m.HandleGet(sc)
		case "3":
			m.HandleDelete(sc)
		case "4":
			m.HandleCompaction(sc)
		case "5":
			return
		default:
			fmt.Println("Niste uneli validnu opciju")
		}
		fmt.Println()
	}
}

func (m *MainMenu) HandlePut(sc *bufio.Scanner) {
	fmt.Print("Unesite kljuc: ")
	sc.Scan()
	key := sc.Text()
	fmt.Print("Unesite podatke: ")
	sc.Scan()
	value := sc.Text()

	err := m.app.Put(key, []byte(value))
	if err != nil {
		fmt.Println("greska: ", err.Error())
	} else {
		fmt.Println("Uspesno dodati podaci.")
	}
}

func (m *MainMenu) HandleGet(sc *bufio.Scanner) {
	fmt.Print("Unesite kljuc: ")
	sc.Scan()
	key := sc.Text()

	data, err := m.app.Get(key)
	if err != nil {
		fmt.Println("greska: ", err.Error())
	} else if data == nil {
		fmt.Println("Podaci nisu pronadjeni.")
	} else {
		fmt.Println(key, " : ", "[", string(data), "]")
	}
}

func (m *MainMenu) HandleDelete(sc *bufio.Scanner) {
	fmt.Print("Unesite kljuc: ")
	sc.Scan()
	key := sc.Text()

	err := m.app.Delete(key)
	if err != nil {
		fmt.Println("greska: ", err.Error())
	} else {
		fmt.Println("Podaci sa kljucem [", key, "] obelezeni za brisanje.")
	}
}

func (m *MainMenu) HandleCompaction(sc *bufio.Scanner) {
	fmt.Print("Potvrda kompakcije (Y/n): ")
	sc.Scan()
	c := sc.Text()

	if c == "n" {
		return
	} else if c != "y" {
		fmt.Println("greska: nepostojeca opcija")
		return
	}

	err := m.app.InitiateCompaction()
	if err != nil {
		fmt.Println("greska: ", err.Error())
	} else {
		fmt.Println("Kompakcija uspesno izvrsena.")
	}
}

func (m *MainMenu) PrintMenu() {
	fmt.Println("---------------------")
	fmt.Println("1 Upisi podatak")
	fmt.Println("2 Pronadji podatak")
	fmt.Println("3 Obrisi podatak")
	fmt.Println("4 Pokreni kompakciju")
	fmt.Println("5 Izadji")
	fmt.Println("---------------------")
}

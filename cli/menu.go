package cli

import (
	"bufio"
	"fmt"
	application "go-touch-grass/internal/app"
	"go-touch-grass/internal/util"
	"os"
)

type MainMenu struct {
	app *application.App
}

func NewMenu(app *application.App) *MainMenu {
	return &MainMenu{app}
}

func (m *MainMenu) PrintMenu() {
	fmt.Println("---------------------")
	fmt.Println("1 Upisi podatak")
	fmt.Println("2 Pronadji podatak")
	fmt.Println("3 Obrisi podatak")
	fmt.Println("4 Pokreni kompakciju")
	fmt.Println("q Izadji")
	fmt.Println("---------------------")
}

func (m *MainMenu) Show() {
	sc := bufio.NewScanner(os.Stdin)
	for {
		m.PrintMenu()
		fmt.Print("Izaberite opciju: ")
		c := util.ScanLowerString(sc)

		switch c {
		case "1":
			m.HandlePut(sc)
		case "2":
			m.HandleGet(sc)
		case "3":
			m.HandleDelete(sc)
		case "4":
			m.HandleCompaction(sc)
		case "q":
			return
		default:
			util.Print("Niste uneli validnu opciju.")
		}
		fmt.Println()
	}
}

func (m *MainMenu) HandlePut(sc *bufio.Scanner) {
	fmt.Print("Unesite kljuc: ")
	key := util.ScanString(sc)
	fmt.Print("Unesite podatke: ")
	value := util.ScanString(sc)

	err := m.app.Put(key, []byte(value))
	if err != nil {
		util.Print("greska: ", err.Error())
	} else {
		util.Print("Uspesno dodati podaci.")
	}
}

func (m *MainMenu) HandleGet(sc *bufio.Scanner) {
	fmt.Print("Unesite kljuc: ")
	key := util.ScanString(sc)

	data, err := m.app.Get(key)
	if err != nil {
		util.Print("greska: ", err.Error())
	} else if data == nil {
		util.Print("Podaci nisu pronadjeni.")
	} else {
		util.Print(key, " : ", "[", string(data), "]")
	}
}

func (m *MainMenu) HandleDelete(sc *bufio.Scanner) {
	fmt.Print("Unesite kljuc: ")
	key := util.ScanString(sc)

	err := m.app.Delete(key)
	if err != nil {
		util.Print("greska: ", err.Error())
	} else {
		util.Print("Podaci sa kljucem [", key, "] obelezeni za brisanje.")
	}
}

func (m *MainMenu) HandleCompaction(sc *bufio.Scanner) {
	fmt.Print("Potvrda kompakcije (Y/n): ")
	c := util.ScanLowerString(sc)

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

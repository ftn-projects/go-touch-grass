package menu

import (
	"bufio"
	"fmt"
	"go-touch-grass/internal/app"
	"go-touch-grass/internal/util"
	"os"
)

type Menu struct {
}

func New() *Menu {
	return &Menu{}
}

func (m *Menu) PrintMenu() {
	fmt.Println("----------- MENI -----------")
	fmt.Println("1 Upisi podatak")
	fmt.Println("2 Pronadji podatak")
	fmt.Println("3 Obrisi podatak")
	fmt.Println("4 Pokreni kompakciju")
	fmt.Println("5 Pokreni ciscenje WAL")
	fmt.Println()
	fmt.Println("q Izadji")
	fmt.Println("----------------------------")
}

func (m *Menu) Show() {
	app, err := app.New()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	sc := bufio.NewScanner(os.Stdin)
	for {
		m.PrintMenu()
		fmt.Print("Izaberite opciju: ")
		c := util.ScanLowerString(sc)

		switch c {
		case "1":
			m.HandlePut(sc, app)
		case "2":
			m.HandleGet(sc, app)
		case "3":
			m.HandleDelete(sc, app)
		case "4":
			m.HandleCompaction(sc, app)
		case "5":
			m.HandleWalCleanup(sc, app)
		case "q":
			return
		default:
			util.Print("Niste uneli validnu opciju.")
		}
		fmt.Println()
		fmt.Println()
	}
}

func (m *Menu) HandlePut(sc *bufio.Scanner, app *app.App) {
	fmt.Print("Unesite kljuc: ")
	key := util.ScanString(sc)
	if key == "" {
		fmt.Println("greska: neispravan kljuc")
		return
	}

	fmt.Print("Unesite podatke: ")
	value := util.ScanString(sc)

	err := app.Put(key, []byte(value))
	if err != nil {
		util.Print("greska: ", err.Error())
	} else {
		util.Print("Uspesno dodati podaci.")
	}
}

func (m *Menu) HandleGet(sc *bufio.Scanner, app *app.App) {
	fmt.Print("Unesite kljuc: ")
	key := util.ScanString(sc)
	if key == "" {
		fmt.Println("greska: neispravan kljuc")
		return
	}

	data, err := app.Get(key)
	if err != nil {
		util.Print("greska: ", err.Error())
	} else if data == nil {
		util.Print("Podaci nisu pronadjeni.")
	} else {
		util.Print("[", key, "]", ":", "[", string(data), "]")
	}
}

func (m *Menu) HandleDelete(sc *bufio.Scanner, app *app.App) {
	fmt.Print("Unesite kljuc: ")
	key := util.ScanString(sc)
	if key == "" {
		fmt.Println("greska: neispravan kljuc")
		return
	}

	err := app.Delete(key)
	if err != nil {
		util.Print("greska: ", err.Error())
	} else {
		util.Print("Podaci sa kljucem [", key, "] obelezeni za brisanje.")
	}
}

func (m *Menu) HandleCompaction(sc *bufio.Scanner, app *app.App) {
	fmt.Print("Unesite nivo za kompakciju: ")
	c := util.ScanInt(sc)
	if c == -1 {
		fmt.Println("greska: niste uneli ceo pozitivan broj")
		return
	}

	err := app.InitiateCompaction(c)
	if err != nil {
		fmt.Println("greska: ", err.Error())
	} else {
		fmt.Println("Kompakcija uspesno izvrsena.")
	}
}

func (m *Menu) HandleWalCleanup(sc *bufio.Scanner, app *app.App) {
	fmt.Print("Sredi WAL (Y/n): ")
	c := util.ScanLowerString(sc)

	if c == "n" {
		return
	} else if c != "y" {
		fmt.Println("greska: nepostojeca opcija")
		return
	}
	app.CleanupWal()
}

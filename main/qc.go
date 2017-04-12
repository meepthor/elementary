package main

import (
	"bufio"
	"encoding/json"
	"flag"
	. "fmt"
	"log"
	"meepthor/qc"
	"os"
)

func WriteFile(fname string, contents []byte) {

	f, err := os.Create(fname)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	_, err = w.Write(contents)
	w.WriteString("\r\n")
	if err != nil {
		log.Println(err)
	}
	w.Flush()
}

func contains(ss []string, t string) bool {
	for _, s := range ss {
		if s == t {
			return true
		}
	}
	return false
}

func DatToJson(dat, dirname string) {

	hdr, lines := qc.Lines(dat)

	var key string

	if contains(hdr, "docid") {
		key = "docid"
	} else if contains(hdr, "begdoc") {
		key = "begdoc"
	}

	counter := 1

	os.Mkdir(dirname, 0700)

	for line := range lines {

		var name string
		var ok bool

		if name, ok = line[key]; !ok {
			name = Sprintf("%08d", counter)
		}

		if jstr, err := json.MarshalIndent(line, " ", " "); err == nil {
			WriteFile(Sprintf("%s/%s.json", dirname, name), jstr)
		}
		counter++
	}

}

func pluckValues(dat string, format qc.Delimiters, columns ...string) {

	hdr, lines := qc.Lines(dat)
	check := make([]string, len(columns))

	for i, c := range columns {
		if !contains(hdr, c) {
			println(c, ": Not found")
		}

		check[i] = c
	}

	if len(check) > 0 {
		if len(check) > 1 {
			qc.Write(format, check)
		}
		for line := range lines {
			if len(check) > 1 {
				buf := make([]string, len(check))
				for i, c := range check {
					if v, ok := line[c]; ok {
						buf[i] = v
					} else {
						buf[i] = ""
					}
				}
				qc.Write(format, buf)
			} else {
				if v, ok := line[check[0]]; ok {
					Println(v)
				} else {
					Println("")
				}
			}
		}
	}
}

type DatReport struct {
	Filename   string
	Lines      int
	LineErrors int
	Header     []string
}

func echoDat(dat string, format qc.Delimiters) {

	hdr, lines := qc.Lines(dat)
	sz := len(hdr)

	write := qc.Writer(format)
	write(hdr)

	for line := range lines {
		row := make([]string, sz)

		for i, h := range hdr {
			if col, ok := line[h]; ok {
				row[i] = col
			} else {
				row[i] = ""
			}
		}
		write(row)
	}
}

func main() {

	var showHeader = flag.Bool("h", false, "List Header")
	var format = flag.String("f", "pipe", "concord csv hat pipe tab")
	var delimiters qc.Delimiters

	flag.Parse()

	switch *format {
	case "concord":
		delimiters = qc.Concordance
	case "pipe":
		delimiters = qc.Piped
	case "hat":
		delimiters = qc.PipeCarat
	case "tab":
		delimiters = qc.Tabbed
	case "csv":
		delimiters = qc.CSV
	default:
		delimiters = qc.Piped
	}

	if *showHeader && flag.NArg() > 0 {
		dat := flag.Args()[0]
		hdr, _ := qc.Lines(dat)
		for _, h := range hdr {
			Println(h)
		}

	} else if flag.NArg() >= 2 {
		dat := flag.Args()[0]
		cols := flag.Args()[1:]
		pluckValues(dat, delimiters, cols...)
	} else if flag.NArg() >= 1 {
		dat := flag.Args()[0]
		echoDat(dat, delimiters)
	} else {
		flag.Usage()
	}

}

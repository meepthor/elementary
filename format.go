package qc

import (
	"bytes"
	"fmt"
	"strings"
)

// Format join []string to string using c and q as delimiter and separator.
func Format(c, q string) func([]string) string {
	return func(row []string) string {
		qcq := fmt.Sprintf("%s%s%s", q, c, q)
		return fmt.Sprintf("%s%s%s", q, strings.Join(row, qcq), q)
	}
}

// FormatCSV join []string to string using c and q as delimiter and separator.
// Adds quotes only if c or q is in column value.
func FormatCSV(c, q string) func([]string) string {
	return func(row []string) string {

		bs := bytes.NewBufferString("")

		addCol := func(col string) {
			if strings.Contains(col, c) || strings.Contains(col, q) {
				col = fmt.Sprintf("%s%s%s", q, col, q)
			}
			bs.WriteString(col)
		}

		i := 0
		max := len(row) - 1
		for ; i < max; i++ {
			addCol(row[i])
			bs.WriteString(c)
		}

		addCol(row[i])
		return bs.String()
	}

}

// Concordance delimiters
var Concordance = Format(Nose, Ear)

// Piped is Pipe,Tilde
var Piped = Format(Pipe, Tilde)

// PipeCarat is Pipe,Carat
var PipeCarat = Format(Pipe, Carat)

// CSV delimiters
var CSV = FormatCSV(Comma, Quote)

// Tabbed use tab as delimiter with no separator
var Tabbed = Format("\t", "")

//var Concord     =   Format(Nose, LeftEar)

// Delimiters function serves as interface
type Delimiters func([]string) string

// []string to string using provided delimiters function
func Line(d Delimiters, row []string) string {
	return fmt.Sprintf("%s\r\n", d(row))
}

// Write writes []string as string to stdout
func Write(d Delimiters, row []string) {
	fmt.Printf("%s\r\n", d(row))
}

// Writer creates a row writer to write []string to stdout
func Writer(d Delimiters) func([]string) {
	return func(row []string) {
		fmt.Printf("%s\r\n", d(row))
	}
}

package qc

import (
    "strings"
    "fmt"
    "bytes"
)


func Format(c, q string) func([]string)string {
    return func(row []string) string {
        qcq := fmt.Sprintf("%s%s%s", q, c, q)
        return fmt.Sprintf("%s%s%s", q, strings.Join(row, qcq), q)    
    }
}

func Format_If(c, q string) func([]string)string {
    return func(row []string) string {

        bs := bytes.NewBufferString("")

        addCol := func(col string) {
            if strings.Contains(col, c) || strings.Contains(col, q){
                col = fmt.Sprintf("%s%s%s", q, col, q)
            }
            bs.WriteString(col)
        }

        i := 0
        max := len(row)-1
        for ; i < max; i++ {
            addCol(row[i])
            bs.WriteString(c)                   
        }
  
        addCol(row[i])
        return bs.String()
    }

}

var Concordance =   Format(Nose, Ear)
var Piped       =   Format(Pipe, Tilde)
var PipeCarat   =   Format(Pipe, Carat)
var CSV         =   Format_If(Comma, Quote)
var Tabbed      =   Format("\t", "")
//var Concord     =   Format(Nose, LeftEar)

type Delimiters func([]string)string 

func Line(d Delimiters, row []string) string {
    return fmt.Sprintf("%s\r\n", d(row))
}

func Write(d Delimiters, row[]string) {
    fmt.Printf("%s\r\n", d(row))
}

func Writer(d Delimiters) func([]string) {
    return func(row []string) {
        fmt.Printf("%s\r\n", d(row))
    }
}

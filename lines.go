package qc

import (
  "os"
  "bufio"
  "log"
  "strings"
  "fmt"
  "bytes"
)



func GetDelimiters(s string) (string, string) {

    qcq := func(c, q string) string { return fmt.Sprintf("%s%s%s", q, c, q)}

    found := func (c , q string) bool {
        return strings.Contains(s, qcq(c, q))
    }
    
    if found(Nose, Ear) { 
        return Nose, Ear 
    //} else if found(Nose, LeftEar) {
    //    return Nose, LeftEar
    } else if found(Pipe, Tilde) {
        return Pipe, Tilde
    } else if found(Pipe, Carat) {
        return Pipe, Carat
    } else if found(Comma, "") {
        return Comma, Quote
    } else if found("\t", "") {
        return "\t", ""
    } else {
    
        // Probably no delimiter 
        first := ""
        if len(s) > 0 {
            first = string(s[0])
        }
        return Pipe, first
    }
    
}


func Zip(hs, cs []string) (map[string]string, bool) {

    row := make(map[string]string, len(hs))

    if len(cs) != len(hs){
        return row, false
    } else {
        for i := 0; i < len(hs); i++ {
            v := cs[i]
            if len(v) > 0 {
                row[hs[i]] = v
            }                
        }
    }

    return row, true    
    
}

func NormHeader(cs []string) []string {
    row := make([]string, len(cs))
    for i:= 0; i < len(cs); i++ {
        c := strings.ToLower(cs[i])
        c = strings.Replace(c, "_", "", -1)
        c = strings.Replace(c, " ", "", -1)
        row[i] = c                       
    }
    return row
}



func OldLineIterator(fname string) <-chan string {

    ch := make(chan string)    
    file, err := os.Open(fname)
    if err != nil {
        log.Fatal(err)        
    }
    
    reader := bufio.NewReader(file)
    scanner := bufio.NewScanner(reader)    

    go func() {

        for scanner.Scan() {
            ch <- scanner.Text()    
        }  

        if err := scanner.Err(); err != nil {
            log.Fatal(err)                
        }

        close(ch) // Remember to close or the loop never ends!
        defer file.Close()
    }()

    return ch
}


func LineIterator(fname string) <-chan string {

    ch := make(chan string)    
    file, err := os.Open(fname)
    if err != nil {
        log.Fatal(err)        
    }
    
    reader := bufio.NewReader(file)
    
    //func (b *Reader) ReadLine() (line []byte, isPrefix bool, err error)
    
       

    go func() {

        var err error
        var txt []byte
        var isPrefix bool
        
        var buf bytes.Buffer
        
        for {
            txt, isPrefix, err = reader.ReadLine()
            buf.Write(txt)
            
            if isPrefix {
                continue
            }
            
            if err == nil {
                ch <- buf.String()
                buf.Reset()
            } else {
                
                s := buf.String()
                if len(s) > 0 {
                    ch <- buf.String()
                }
                
                close(ch) // Remember to close or the loop never ends!
                break
            }
        }
        
        defer file.Close()
    }()

    return ch
}


func Lines(fname string) ([]string, <-chan map[string]string) {
    
    lines := LineIterator(fname)
    first := strings.Replace(<-lines, Bom, "", 1)    
    
    c, q := GetDelimiters(first)        
    hdr := NormHeader(Columns(first, c, q))
        
    rc := make(chan map[string]string)
    rowCount := 1
        
    go func(){
        for line := range lines {
            rowCount++
            cols := Columns(line, c, q)
            row, ok := Zip(hdr, cols)
            if ok {
                rc <- row
            } else {
                log.Printf(
                    "[ %s ] => Line: %d, Row: %d, Header: %d\n", 
                    fname, rowCount, len(cols), len(hdr))
            }
        }    
        close(rc)
    }()
    
    return hdr, rc

}


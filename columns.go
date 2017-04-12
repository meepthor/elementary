package qc

import (
	. "fmt"
	"strings"
)

// Columns splits string in into []string using comma and quote as separator and delimiter.
func Columns(row, comma, quote string) []string {

	undouble := func(s string) string {
		doubled := Sprintf("%s%s", quote, quote)

		replace := func(s string) string {
			return strings.Replace(s, doubled, quote, -1)
		}

		process := func(s string) string {
			for strings.Contains(s, doubled) {
				s = replace(s)
			}
			return s
		}
		return process(s)
	}

	trim := func(s string) string {
		if len(s) > 1 {
			if strings.HasPrefix(s, quote) {
				if strings.HasSuffix(s, quote) {
					return s[1 : len(s)-1]
				}
			}
		}
		return s
	}

	cstream := func(s string) <-chan string {
		cs := make(chan string)
		go func() {
			for strings.Contains(s, comma) {
				found := strings.Index(s, comma)
				cs <- s[:found]
				s = s[found+1:]
			}
			cs <- s // whether or not len(s) > 0
			close(cs)
		}()
		return cs
	}

	qstream := func(s string) <-chan string {

		qs := make(chan string)

		var buf = make([]string, 0)
		var yield = func() { qs <- strings.Join(buf, comma); buf = nil }
		var quoted = false

		go func() {
			for c := range cstream(s) {
				buf = append(buf, c)
				if strings.HasSuffix(c, quote) {
					if quoted || (strings.HasPrefix(c, quote) && len(c) > 1) {
						yield()
						quoted = false
					} else {
						quoted = true
					}
				} else if !quoted {
					if strings.HasPrefix(c, quote) {
						quoted = true
					} else {
						yield()
					}
				}
			}
			if len(buf) > 0 {
				yield()
			}
			close(qs)
		}()
		return qs
	}

	if quote == "" {
		return strings.Split(row, comma)
	}

	buffer := make([]string, 0)
	for q := range qstream(row) {
		buffer = append(buffer, undouble(trim(q)))
	}

	return buffer

}

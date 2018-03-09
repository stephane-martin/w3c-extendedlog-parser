package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	parser "github.com/stephane-martin/w3c-extendedlog-parser"
)

var filename = flag.String("fname", "", "path to the file to parse")

func main() {
	flag.Parse()
	*filename = strings.TrimSpace(*filename)
	if len(*filename) == 0 {
		flag.Usage()
		os.Exit(0)
	}
	f, err := os.Open(*filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening '%s': %s\n", *filename, err)
		os.Exit(-1)
	}
	defer f.Close()

	p := parser.NewFileParser(f)
	err = p.ParseHeader()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error building parser:", err)
		os.Exit(-1)
	}
	var l *parser.Line
	for {
		l, err = p.Next()
		if l == nil || err != nil {
			break
		}
		b, err := l.MarshalJSON()
		if err == nil {
			fmt.Println(string(b))
		}
	}
	if err != nil && err != io.EOF {
		fmt.Fprintln(os.Stderr, err)
	}

}

package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spaolacci/murmur3"
	"github.com/spf13/cobra"
	parser "github.com/stephane-martin/w3c-extendedlog-parser"
)

// uniqueCmd represents the unique command
var uniqueCmd = &cobra.Command{
	Use:   "unique",
	Short: "Count the number of unique lines",
	Run: func(cmd *cobra.Command, args []string) {
		if len(input) == 0 {
			fatal(errors.New("specify an input directory"))
		}
		curdir, err := os.Getwd()
		fatal(err)
		curdir, err = filepath.Abs(curdir)
		fatal(err)
		input, err = filepath.Abs(input)
		fatal(err)

		inputFiles, err := findFiles(input, extension)
		fatal(err)
		if len(inputFiles) == 0 {
			fmt.Fprintln(os.Stderr, "No file to process.")
			return
		}
		uniqueHashes := make(map[string]bool)
		var total uint64
		for _, file := range inputFiles {
			err = uniqueFile(file, &uniqueHashes, &total)
			fatal(err)
			fmt.Fprintf(os.Stderr, "%d unique lines / %d\n", len(uniqueHashes), total)
		}

	},
}

func uniqueFile(fname string, uniques *map[string]bool, total *uint64) error {
	f, err := os.Open(fname)
	if err != nil {
		return err
	}
	defer f.Close()
	p := parser.NewFileParser(f)
	err = p.ParseHeader()
	if err != nil {
		return err
	}
	fields := p.FieldNames()
	var line *parser.Line
	var field string
	var h murmur3.Hash128

	for {
		line, err = p.NextTo(line)
		if line == nil || err != nil {
			break
		}
		(*total)++
		h = murmur3.New128()
		for _, field = range fields {
			_, err = h.Write([]byte(line.GetAsString(field) + " "))
			if err != nil {
				return err
			}
		}
		(*uniques)[string(h.Sum(nil))] = true
	}
	return nil

}

func init() {
	rootCmd.AddCommand(uniqueCmd)
	uniqueCmd.Flags().StringVar(&input, "input", "", "input directory")
	uniqueCmd.Flags().StringVar(&extension, "ext", "log", "only select input files with that extension")
}

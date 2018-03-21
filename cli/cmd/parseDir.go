package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var input string
var output string
var extension string

var parseDirCmd = &cobra.Command{
	Use:   "parse-dir",
	Short: "Parse every file of some input directory",
	Run: func(cmd *cobra.Command, args []string) {
		if len(input) == 0 {
			fatal(errors.New("specify an input directory"))
		}
		if jsonExport && csvExport {
			fatal(errors.New("--json and --csv are exclusive"))
		}
		if !jsonExport && !csvExport {
			jsonExport = true
		}
		curdir, err := os.Getwd()
		fatal(err)
		curdir, err = filepath.Abs(curdir)
		fatal(err)
		input, err = filepath.Abs(filepath.Join(curdir, input))
		fatal(err)
		outputInfos, err := os.Stat(output)
		if err != nil && !os.IsNotExist(err) {
			// error when stat'ing the output directory
			fatal(err)
		}
		if err == nil && !outputInfos.IsDir() {
			// output directory exists but is not a directory
			fatal(errors.New("output is not a directory"))
		}
		output, err = filepath.Abs(output)
		fatal(err)

		inputFiles := make([]string, 0)
		if len(extension) > 0 {
			extension = "." + extension
		}
		err = filepath.Walk(input, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			if info.Mode().IsRegular() && (len(extension) == 0 || filepath.Ext(path) == extension) {
				path, err = filepath.Abs(path)
				if err != nil {
					return err
				}
				inputFiles = append(inputFiles, path)
			}
			return nil
		})
		fatal(err)

		if len(inputFiles) == 0 {
			fmt.Fprintln(os.Stderr, "No file to process.")
			return
		}
		fmt.Fprintln(os.Stderr, "Will process the following files")
		for _, fname := range inputFiles {
			fmt.Fprintf(os.Stderr, "- %s\n", fname)
		}
		fmt.Fprintln(os.Stderr)

		for _, fname := range inputFiles {
			var inFile, outFile *os.File
			outFname := ""
			fmt.Fprintln(os.Stderr, "Processing:", fname)

			relpath, err := filepath.Rel(input, fname)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				fmt.Fprintln(os.Stderr)
				continue
			}

			inFile, err = os.Open(fname)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				fmt.Fprintln(os.Stderr)
				continue
			}

			var out io.Writer
			if len(output) == 0 {
				out = os.Stdout
				outFile = nil
			} else {
				outFname = filepath.Join(output, relpath)
				if jsonExport {
					outFname = outFname + ".jsonlines"
				}
				if csvExport {
					outFname = outFname + ".csv"
				}
				outDir := filepath.Dir(outFname)
				os.MkdirAll(outDir, 0755)
				outFile, err = os.Create(outFname)
				if err != nil {
					inFile.Close()
					fmt.Fprintln(os.Stderr, err)
					fmt.Fprintln(os.Stderr)
					continue
				}
				out = outFile
			}

			err = doParse(inFile, out, jsonExport, csvExport, suffix)

			if outFile != nil {
				outFile.Close()
			}
			inFile.Close()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			} else if len(outFname) > 0 {
				fmt.Fprintln(os.Stderr, "Written:", outFname)
			}
			fmt.Fprintln(os.Stderr)
		}

	},
}

func init() {
	rootCmd.AddCommand(parseDirCmd)
	parseDirCmd.Flags().StringVar(&input, "input", "", "input directory")
	parseDirCmd.Flags().StringVar(&output, "output", "", "output directory (if empty, use stdout)")
	parseDirCmd.Flags().StringVar(&extension, "ext", "log", "only select input files with that extension")
	parseDirCmd.Flags().BoolVar(&jsonExport, "json", false, "print the logs as JSON")
	parseDirCmd.Flags().BoolVar(&csvExport, "csv", false, "print the logs as CSV")
	parseDirCmd.Flags().BoolVar(&suffix, "suffix", false, "when exporting to CSV, suffix the field names with data type")
}

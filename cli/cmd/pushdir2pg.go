package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx"
	"github.com/spf13/cobra"
)

// pushdir2pgCmd represents the pushdir2pg command
var pushdir2pgCmd = &cobra.Command{
	Use:   "pushdir2pg",
	Short: "Parse all files in some directory and push events to pgsql",
	Run: func(cmd *cobra.Command, args []string) {
		if parallel == 0 {
			parallel = 1
		}
		if batchsize == 0 {
			batchsize = 5000
		}
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
		fmt.Fprintln(os.Stderr, "Will process the following files")
		for _, fname := range inputFiles {
			fmt.Fprintf(os.Stderr, "- %s\n", fname)
		}
		fmt.Fprintln(os.Stderr)

		dbURI = strings.TrimSpace(dbURI)
		if len(dbURI) == 0 {
			fatal(errors.New("Empty uri"))
		}
		config, err := pgx.ParseConnectionString(dbURI)
		fatal(err)
		pool, err := pgx.NewConnPool(pgx.ConnPoolConfig{
			ConnConfig:     config,
			MaxConnections: int(parallel),
		})
		fatal(err)
		defer pool.Close()

		uploadFilesPG(inputFiles, pool, uint(parallel), batchsize)

	},
}

func init() {
	rootCmd.AddCommand(pushdir2pgCmd)
	pushdir2pgCmd.Flags().StringVar(&input, "input", "", "input directory")
	pushdir2pgCmd.Flags().StringVar(&extension, "ext", "log", "only select input files with that extension")
	pushdir2pgCmd.Flags().StringVar(&tableName, "tablename", "accesslogs", "name of pg table to push events to")
	pushdir2pgCmd.Flags().StringVar(&dbURI, "uri", "", "the URI of the postgresql server to connect to")
	pushdir2pgCmd.Flags().Uint8Var(&parallel, "parallel", 1, "number of parallel injectors")
	pushdir2pgCmd.Flags().IntVar(&batchsize, "batchsize", 5000, "batch size for postgresql INSERT")
}

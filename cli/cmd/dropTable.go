package cmd

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx"
	"github.com/spf13/cobra"
)

var dropTableCmd = &cobra.Command{
	Use:   "drop-table",
	Short: "drop table from postgres",
	Run: func(cmd *cobra.Command, args []string) {
		dbURI = strings.TrimSpace(dbURI)
		tableName = strings.TrimSpace(tableName)
		if len(dbURI) == 0 || len(tableName) == 0 {
			fatal(errors.New("Empty uri or tablename"))
			os.Exit(-1)
		}
		if !validName(tableName) {
			fatal(errors.New("invalid table name"))
		}
		dropStmt = fmt.Sprintf("DROP TABLE %s;", tableName)

		config, err := pgx.ParseConnectionString(dbURI)
		fatal(err)
		conn, err := pgx.Connect(config)
		fatal(err)
		defer conn.Close()

		fmt.Fprintf(os.Stderr, "executing: %s\n", dropStmt)
		_, err = conn.Exec(dropStmt)
		fatal(err)
		fmt.Fprintf(os.Stderr, "table '%s' has been dropped\n", tableName)
	},
}

func init() {
	rootCmd.AddCommand(dropTableCmd)
	dropTableCmd.Flags().StringVar(&tableName, "tablename", "accesslogs", "name of table to be dropped")
	dropTableCmd.Flags().StringVar(&dbURI, "uri", "", "the URI of the postgresql server to connect to")
}

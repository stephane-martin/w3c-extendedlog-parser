package cmd

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx"
	"github.com/spf13/cobra"
)

var dropStmt = "DROP DATABASE %s;"

var dropDatabaseCmd = &cobra.Command{
	Use:   "drop-database",
	Short: "drop a database from postgresql",
	Run: func(cmd *cobra.Command, args []string) {
		dbURI = strings.TrimSpace(dbURI)
		dbName = strings.TrimSpace(dbName)
		if len(dbURI) == 0 || len(dbName) == 0 {
			fatal(errors.New("Empty uri or dbname"))
			os.Exit(-1)
		}
		if !validName(dbName) {
			fatal(errors.New("invalid database name"))
		}
		dropStmt = fmt.Sprintf(dropStmt, dbName)

		config, err := pgx.ParseConnectionString(dbURI)
		fatal(err)
		if config.Database == "" {
			config.Database = "postgres"
		}
		conn, err := pgx.Connect(config)
		fatal(err)
		defer conn.Close()

		fmt.Fprintf(os.Stderr, "executing: %s\n", dropStmt)
		_, err = conn.Exec(dropStmt)
		fatal(err)
		fmt.Fprintf(os.Stderr, "database '%s' has been dropped\n", dbName)
	},
}

func init() {
	rootCmd.AddCommand(dropDatabaseCmd)
	dropDatabaseCmd.Flags().StringVar(&dbURI, "uri", "", "the URI of the postgresql server to connect to")
	dropDatabaseCmd.Flags().StringVar(&dbName, "dbname", "", "the name of the database to be dropped")
}

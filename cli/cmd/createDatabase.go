package cmd

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/jackc/pgx"
	"github.com/spf13/cobra"
)

var dbURI string
var dbName string
var owner string

func fatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(-1)
	}
}

var validName = regexp.MustCompile(`^[a-zA-Z0-9]+$`).MatchString

var createStmt = "CREATE DATABASE %s WITH"
var optsStmt = "ENCODING='UTF8' LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8'"
var ownerStmt = "OWNER=%s"

// createDatabaseCmd represents the createDatabase command
var createDatabaseCmd = &cobra.Command{
	Use:   "create-database",
	Short: "create database in postgresql",
	Run: func(cmd *cobra.Command, args []string) {
		dbURI = strings.TrimSpace(dbURI)
		dbName = strings.TrimSpace(dbName)
		if len(dbURI) == 0 || len(dbName) == 0 {
			fatal(errors.New("Empty uri or dbname"))
		}
		if !validName(dbName) {
			fatal(errors.New("invalid database name"))
		}

		config, err := pgx.ParseConnectionString(dbURI)
		fatal(err)
		if config.Database == "" {
			config.Database = "postgres"
		}
		conn, err := pgx.Connect(config)
		fatal(err)
		defer conn.Close()

		createStmt = fmt.Sprintf(createStmt, dbName)
		if len(owner) > 0 {
			if !validName(owner) {
				fatal(errors.New("invalid owner name"))
			}
			createStmt = createStmt + " " + fmt.Sprintf(ownerStmt, owner)
		}
		createStmt = createStmt + " " + optsStmt + ";"
		fmt.Fprintf(os.Stderr, "executing: %s\n", createStmt)
		_, err = conn.Exec(createStmt)
		fatal(err)
		fmt.Fprintf(os.Stderr, "database '%s' has been created\n", dbName)
	},
}

func init() {
	rootCmd.AddCommand(createDatabaseCmd)
	createDatabaseCmd.Flags().StringVar(&dbURI, "uri", "", "the URI of the postgresql server to connect to")
	createDatabaseCmd.Flags().StringVar(&dbName, "dbname", "", "the name of the database to be created")
	createDatabaseCmd.Flags().StringVar(&owner, "owner", "", "database owner")

}

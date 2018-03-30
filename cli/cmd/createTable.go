package cmd

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx"
	"github.com/spf13/cobra"
	parser "github.com/stephane-martin/w3c-extendedlog-parser"
)

var tableName string
var noIndex bool

func pgKey(key string) string {
	key = strings.Replace(key, "-", "_", -1)
	key = strings.Replace(key, "(", "$", -1)
	key = strings.Replace(key, ")", "$", -1)
	return key
}

var createTableCmd = &cobra.Command{
	Use:   "create-table",
	Short: "Create a table in postgres with an adequate schema to store access logs",
	Run: func(cmd *cobra.Command, args []string) {
		fieldsLine = strings.TrimSpace(fieldsLine)
		fname = strings.TrimSpace(fname)

		if len(fieldsLine) == 0 && len(fname) == 0 {
			fatal(errors.New("Specify fields with --filename or --fields"))
		}
		if len(fieldsLine) != 0 && len(fname) != 0 {
			fatal(errors.New("--fields and --filename options are exclusive"))
		}
		if len(fieldsLine) > 0 {
			fieldNames = strings.Split(fieldsLine, " ")
		} else {
			f, err := os.Open(fname)
			fatal(err)
			p := parser.NewFileParser(f)
			err = p.ParseHeader()
			f.Close()
			fatal(err)
			fieldNames = p.FieldNames()
		}
		if len(fieldNames) == 0 {
			fatal(errors.New("field names not found"))
		}

		columns := make(map[string]string, len(fieldNames)+1)
		columns["id"] = "BIGSERIAL PRIMARY KEY"
		for _, name := range fieldNames {
			switch parser.GuessType(name) {
			case parser.MyDate:
				columns[pgKey(name)] = "DATE NULL"
			case parser.MyIP:
				columns[pgKey(name)] = "INET NULL"
			case parser.MyTime:
				columns[pgKey(name)] = "TIME NULL"
			case parser.MyTimestamp:
				columns[pgKey(name)] = "TIMESTAMP WITH TIME ZONE NULL"
			case parser.MyURI:
				columns[pgKey(name)] = "TEXT DEFAULT '' NOT NULL"
			case parser.Float64:
				columns[pgKey(name)] = "DOUBLE PRECISION NULL"
			case parser.Int64:
				columns[pgKey(name)] = "BIGINT NULL"
			case parser.Bool:
				columns[pgKey(name)] = "BOOLEAN NULL"
			case parser.String:
				columns[pgKey(name)] = "TEXT DEFAULT '' NOT NULL"
			default:
				columns[pgKey(name)] = "TEXT DEFAULT '' NOT NULL"
			}
		}

		if columns["gmttime"] == "" {
			fieldNames = append([]string{"gmttime"}, fieldNames...)
			columns["gmttime"] = "TIMESTAMP WITH TIME ZONE NULL"
		}
		fieldNames = append([]string{"id"}, fieldNames...)

		createStmt := "CREATE TABLE %s (\n"
		for _, name := range fieldNames {
			createStmt += fmt.Sprintf("    %s %s,\n", pgKey(name), columns[pgKey(name)])
		}
		// remove last ,
		createStmt = strings.Trim(createStmt, ",\n")
		createStmt += ");"

		dbURI = strings.TrimSpace(dbURI)
		tableName = strings.TrimSpace(tableName)
		if len(dbURI) == 0 || len(tableName) == 0 {
			fatal(errors.New("Empty uri or tablename"))
		}
		if !validName(tableName) {
			fatal(errors.New("invalid table name"))
		}

		config, err := pgx.ParseConnectionString(dbURI)
		fatal(err)
		conn, err := pgx.Connect(config)
		fatal(err)
		defer conn.Close()

		createStmt = fmt.Sprintf(createStmt, tableName)
		fmt.Fprintln(os.Stderr, createStmt)
		_, err = conn.Exec(createStmt)
		fatal(err)
		fmt.Fprintf(os.Stderr, "table '%s' has been created\n", tableName)

		if noIndex {
			return
		}

		createIndexStmt := ""
	Loop:
		for _, name := range fieldNames {
			switch parser.GuessType(name) {
			case parser.MyDate, parser.MyTime, parser.MyTimestamp:
				createIndexStmt = fmt.Sprintf("CREATE INDEX %s_idx ON %s USING BRIN (%s);", pgKey(name), tableName, pgKey(name))

			case parser.MyIP:
				createIndexStmt = fmt.Sprintf("CREATE INDEX %s_idx ON %s USING GIST (%s inet_ops);", pgKey(name), tableName, pgKey(name))

			case parser.Float64, parser.Int64, parser.Bool:
				createIndexStmt = fmt.Sprintf("CREATE INDEX %s_idx ON %s (%s);", pgKey(name), tableName, pgKey(name))

			case parser.String, parser.MyURI:
				if name == "id" {
					continue Loop
				}
				createIndexStmt = fmt.Sprintf("CREATE INDEX %s_idx ON %s USING HASH (%s);", pgKey(name), tableName, pgKey(name))

			default:
				continue Loop
			}
			fmt.Fprintln(os.Stderr, createIndexStmt)
			_, err = conn.Exec(createIndexStmt)
			fatal(err)
			fmt.Fprintf(os.Stderr, "Index has been created on %s\n", name)
		}

		for _, name := range fieldNames {
			if name == "cs(user-agent)" {
				createIndexStmt = fmt.Sprintf("CREATE INDEX full_useragent_idx ON %s USING GIN (to_tsvector('english', %s));", tableName, pgKey(name))
				fmt.Fprintln(os.Stderr, createIndexStmt)
				_, err = conn.Exec(createIndexStmt)
				fatal(err)
				fmt.Fprintln(os.Stderr, "full text index has been created on cs(User-Agent)")
			}
		}

	},
}

func init() {
	rootCmd.AddCommand(createTableCmd)
	createTableCmd.Flags().StringVar(&tableName, "tablename", "accesslogs", "name of table to be created in pgsql")
	createTableCmd.Flags().StringVar(&fieldsLine, "fields", "", "specify the fields that will be present in the access logs")
	createTableCmd.Flags().StringVar(&fname, "filename", "", "specify the log file from which to extract the fields")
	createTableCmd.Flags().StringVar(&dbURI, "uri", "", "the URI of the postgresql server to connect to")
	createTableCmd.Flags().BoolVar(&noIndex, "noindex", false, "if set, do not create indices in pgsql")
}

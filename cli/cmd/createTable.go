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
var partitionKey string
var parentPartitionKey string
var rangeStart string
var rangeEnd string
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
		var fieldsNames []string
		fieldsLine = strings.TrimSpace(fieldsLine)
		fname := strings.TrimSpace(filename)

		if len(fieldsLine) == 0 && len(fname) == 0 {
			fatal(errors.New("Specify fields with --filename or --fields"))
		}
		if len(fieldsLine) != 0 && len(fname) != 0 {
			fatal(errors.New("--fields and --filename options are exclusive"))
		}
		if len(parentPartitionKey) > 0 && len(partitionKey) > 0 {
			fatal(errors.New("--partition and --parent are exclusive"))
		}
		if len(parentPartitionKey) > 0 && (len(rangeStart) == 0 || len(rangeEnd) == 0) {
			fatal(errors.New("if --parent is set, --start and --end must be specified too"))
		}
		if len(fieldsLine) > 0 {
			fieldsNames = strings.Split(fieldsLine, " ")
		} else {
			f, err := os.Open(fname)
			fatal(err)
			p := parser.NewFileParser(f)
			err = p.ParseHeader()
			f.Close()
			fatal(err)
			fieldsNames = p.FieldNames()
		}
		if len(fieldsNames) == 0 {
			fatal(errors.New("field names not found"))
		}
		hasGMT := false
		for _, name := range fieldsNames {
			if name == "gmttime" {
				hasGMT = true
				break
			}
		}
		if !hasGMT {
			fieldsNames = append([]string{"gmttime"}, fieldsNames...)
		}
		fieldsNames = append([]string{"id"}, fieldsNames...)
		createStmt := ""
		if len(parentPartitionKey) == 0 {
			createStmt = buildCreateStmt(tableName, fieldsNames, partitionKey)
		} else {
			createStmt = buildCreateChildStmt(tableName, parentPartitionKey, rangeStart, rangeEnd)
		}

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

		fmt.Fprintln(os.Stderr, createStmt)
		_, err = conn.Exec(createStmt)
		fatal(err)
		fmt.Fprintf(os.Stderr, "table '%s' has been created\n", tableName)

		if noIndex || len(partitionKey) > 0 {
			return
		}

		createIndexStmt := ""

		for _, fieldName := range fieldsNames {
			createIndexStmt = buildIndexStmt(tableName, fieldName, len(parentPartitionKey) > 0)
			if len(createIndexStmt) == 0 {
				continue
			}
			fmt.Fprintln(os.Stderr, createIndexStmt)
			_, err = conn.Exec(createIndexStmt)
			fatal(err)
			fmt.Fprintf(os.Stderr, "Index has been created on %s\n", fieldName)
		}
	},
}

func init() {
	rootCmd.AddCommand(createTableCmd)
	createTableCmd.Flags().StringVar(&tableName, "tablename", "accesslogs", "name of table to be created in pgsql")
	createTableCmd.Flags().StringVar(&fieldsLine, "fields", "", "specify the fields that will be present in the access logs")
	createTableCmd.Flags().StringVar(&filename, "filename", "", "specify the log file from which to extract the fields")
	createTableCmd.Flags().StringVar(&dbURI, "uri", "", "the URI of the postgresql server to connect to")
	createTableCmd.Flags().BoolVar(&noIndex, "noindex", false, "if set, do not create indices in pgsql")
	createTableCmd.Flags().StringVar(&partitionKey, "partition", "", "if set, create a partitioned table using the given column name")
	createTableCmd.Flags().StringVar(&parentPartitionKey, "parent", "", "if set, create the table as a child partition of that parent")
	createTableCmd.Flags().StringVar(&rangeStart, "start", "", "range start for the child partition")
	createTableCmd.Flags().StringVar(&rangeEnd, "end", "", "range end for the child partition")
}

func buildCreateChildStmt(tName string, parent string, start string, end string) string {
	return fmt.Sprintf(
		"CREATE TABLE %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s');",
		tName, parent, start, end,
	)
}

func buildCreateStmt(tName string, fNames []string, pKey string) string {
	columns := make(map[string]string, len(fNames)+1)
	for _, fName := range fNames {
		if fName == "id" && len(pKey) == 0 {
			columns["id"] = "UUID PRIMARY KEY"
			continue
		}
		if fName == "id" {
			columns["id"] = "UUID"
			continue
		}
		if fName == "gmttime" {
			columns["gmttime"] = "TIMESTAMP WITH TIME ZONE NULL"
			continue
		}
		switch parser.GuessType(fName) {
		case parser.MyDate:
			columns[pgKey(fName)] = "DATE NULL"
		case parser.MyIP:
			columns[pgKey(fName)] = "INET NULL"
		case parser.MyTime:
			columns[pgKey(fName)] = "TIME NULL"
		case parser.MyTimestamp:
			columns[pgKey(fName)] = "TIMESTAMP WITH TIME ZONE NULL"
		case parser.MyURI:
			columns[pgKey(fName)] = "TEXT DEFAULT '' NOT NULL"
		case parser.Float64:
			columns[pgKey(fName)] = "DOUBLE PRECISION NULL"
		case parser.Int64:
			columns[pgKey(fName)] = "BIGINT NULL"
		case parser.Bool:
			columns[pgKey(fName)] = "BOOLEAN NULL"
		case parser.String:
			columns[pgKey(fName)] = "TEXT DEFAULT '' NOT NULL"
		default:
			columns[pgKey(fName)] = "TEXT DEFAULT '' NOT NULL"
		}
	}

	createStmt := "CREATE TABLE %s (\n"
	for _, fName := range fNames {
		if fName == "x-virus-id" || fName == "x-bluecoat-application-name" || fName == "x-bluecoat-application-operation" {
			continue
		}
		createStmt += fmt.Sprintf("    %s %s,\n", pgKey(fName), columns[pgKey(fName)])
	}
	// remove last ,
	createStmt = strings.Trim(createStmt, ",\n")
	// add a PARTITION if requested
	if len(pKey) > 0 {
		createStmt += fmt.Sprintf(") PARTITION BY RANGE (%s);", pKey)
	} else {
		createStmt += ");"
	}
	return fmt.Sprintf(createStmt, tName)
}

func buildIndexStmt(tName string, fName string, isChild bool) string {
	switch parser.GuessType(fName) {
	case parser.MyDate, parser.MyTime, parser.MyTimestamp:
		return fmt.Sprintf("CREATE INDEX %s_%s_idx ON %s (%s);", tName, pgKey(fName), tName, pgKey(fName))

	case parser.MyIP:
		return fmt.Sprintf("CREATE INDEX %s_%s_idx ON %s USING GIST (%s inet_ops);", tName, pgKey(fName), tName, pgKey(fName))

	case parser.Float64, parser.Int64, parser.Bool:
		return fmt.Sprintf("CREATE INDEX %s_%s_idx ON %s (%s);", tName, pgKey(fName), tName, pgKey(fName))

	case parser.String, parser.MyURI:
		if fName == "id" {
			// primary key
			if isChild {
				return fmt.Sprintf("CREATE INDEX %s_%s_idx ON %s (%s);", tName, pgKey(fName), tName, pgKey(fName))
			}
			return ""
		}
		if fName == "cs-uri-query" || fName == "cs(referer)" || fName == "cs-uri-path" {
			// fields too large for a BTREE index
			return ""
		}
		if fName == "x-virus-id" || fName == "x-bluecoat-application-name" || fName == "x-bluecoat-application-operation" {
			// fields not so interesting
			return ""
		}
		if fName == "cs(user-agent)" {
			return fmt.Sprintf(
				"CREATE INDEX %s_full_useragent_idx ON %s USING GIN (to_tsvector('english', %s));",
				tName,
				tName,
				pgKey(fName),
			)
		}
		return fmt.Sprintf("CREATE INDEX %s_%s_idx ON %s (%s);", tName, pgKey(fName), tName, pgKey(fName))
	default:
		return ""
	}
}

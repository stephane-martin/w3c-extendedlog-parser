package cmd

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgio"
	"github.com/jackc/pgx/pgtype"
	"github.com/spf13/cobra"
	parser "github.com/stephane-martin/w3c-extendedlog-parser"
)

const (
	usecsPerHour    = 3600000000
	usecsPerMinute  = 60000000
	usecsPerSec     = 1000000
	nanosecsPerUsec = 1000
)

var push2pgCmd = &cobra.Command{
	Use:   "push2pg",
	Short: "Parse accesslog files and push events to postgres",
	Run: func(cmd *cobra.Command, args []string) {
		if len(fnames) == 0 {
			fatal(errors.New("specify the files to be parsed"))
		}
		dbURI = strings.TrimSpace(dbURI)
		if len(dbURI) == 0 {
			fatal(errors.New("Empty uri"))
		}
		config, err := pgx.ParseConnectionString(dbURI)
		fatal(err)
		conn, err := pgx.Connect(config)
		fatal(err)
		defer conn.Close()

		for _, fname := range fnames {
			rows := make([][]interface{}, 0, 1000)
			fname = strings.TrimSpace(fname)
			f, err := os.Open(fname)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error opening '%s': %s\n", fname, err)
				continue
			}
			defer f.Close()

			p := parser.NewFileParser(f)
			err = p.ParseHeader()
			if err != nil {
				fmt.Fprintln(os.Stderr, "Error building parser:", err)
				continue
			}

			columnNames := make([]string, 0, len(p.FileHeader.FieldNames))
			types := make(map[string]parser.Kind, len(p.FileHeader.FieldNames))
			for _, name := range p.FileHeader.FieldNames {
				columnNames = append(columnNames, pgKey(name))
				types[name] = parser.GuessType(name)
			}

			var l *parser.Line
			for {
				row := make([]interface{}, 0, len(p.FileHeader.FieldNames))
				l, err = p.Next()
				if l == nil || err != nil {
					break
				}
				for _, name := range p.FileHeader.FieldNames {
					val := l.Fields[name]
					if val == nil {
						// append default value for that type
						row = append(row, defaultValue(types[name]))
						continue
					}
					// append converted type
					row = append(row, pgConvert(types[name], val))
				}
				rows = append(rows, row)
				if len(rows) >= 1000 {
					_, err = conn.CopyFrom(
						pgx.Identifier{tableName},
						columnNames,
						pgx.CopyFromRows(rows),
					)
					fatal(err)
					rows = make([][]interface{}, 0, 1000)
				}
			}
			if len(rows) > 0 {
				_, err = conn.CopyFrom(
					pgx.Identifier{tableName},
					columnNames,
					pgx.CopyFromRows(rows),
				)
				fatal(err)
			}
		}

	},
}

type MyMyTime struct {
	parser.Time
}

func (src *MyMyTime) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	return pgio.AppendInt64(buf, (int64(src.Hour)*usecsPerHour)+(int64(src.Minute)*usecsPerMinute)+(int64(src.Second)*usecsPerSec)+(int64(src.Nanosecond)/nanosecsPerUsec)), nil
}

func defaultValue(t parser.Kind) interface{} {
	switch t {
	case parser.MyDate:
		return &pgtype.Date{Status: pgtype.Present, Time: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)}
	case parser.MyIP:
		return &pgtype.Inet{Status: pgtype.Present, IPNet: &net.IPNet{IP: net.IPv4(0, 0, 0, 0)}}
	case parser.MyTime:
		return MyMyTime{Time: parser.Time{}}
	case parser.MyTimestamp:
		return &pgtype.Timestamptz{Status: pgtype.Present, Time: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)}
	case parser.MyURI:
		return ""
	case parser.Float64:
		return float64(0)
	case parser.Int64:
		return int64(0)
	case parser.Bool:
		return false
	case parser.String:
		return ""
	}
	return ""
}

func pgConvert(t parser.Kind, value interface{}) interface{} {
	switch t {
	case parser.MyDate:
		v := value.(parser.Date)
		return time.Date(v.Year, v.Month, v.Day, 0, 0, 0, 0, time.UTC)
	case parser.MyIP:
		inet := &pgtype.Inet{}
		inet.Set(value.(net.IP))
		return inet
	case parser.MyTime:
		return &MyMyTime{Time: value.(parser.Time)}
	case parser.MyTimestamp:
		return &pgtype.Timestamptz{Status: pgtype.Present, Time: value.(time.Time)}
	case parser.MyURI:
		return value.(string)
	case parser.Float64:
		return value.(float64)
	case parser.Int64:
		return value.(int64)
	case parser.Bool:
		return value.(bool)
	case parser.String:
		return value.(string)
	}
	return value.(string)
}

func init() {
	rootCmd.AddCommand(push2pgCmd)
	push2pgCmd.Flags().StringArrayVar(&fnames, "filename", []string{}, "the files to parse")
	push2pgCmd.Flags().StringVar(&tableName, "tablename", "accesslogs", "name of pg table to push events to")
	push2pgCmd.Flags().StringVar(&dbURI, "uri", "", "the URI of the postgresql server to connect to")
}

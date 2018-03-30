package cmd

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgio"
	"github.com/jackc/pgx/pgtype"
	unidecode "github.com/mozillazg/go-unidecode"
	"github.com/spf13/cobra"
	parser "github.com/stephane-martin/w3c-extendedlog-parser"
	"golang.org/x/text/encoding/charmap"
)

const (
	usecsPerHour    = 3600000000
	usecsPerMinute  = 60000000
	usecsPerSec     = 1000000
	nanosecsPerUsec = 1000
)

var parallel uint8

var isoDecoder = charmap.ISO8859_15.NewDecoder()

var push2pgCmd = &cobra.Command{
	Use:   "push2pg",
	Short: "Parse accesslog files and push events to postgres",
	Run: func(cmd *cobra.Command, args []string) {
		if parallel == 0 {
			parallel = 1
		}
		if len(fnames) == 0 {
			fatal(errors.New("specify the files to be parsed"))
		}
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
		uploadFilesPG(fnames, pool, uint(parallel))
	},
}

func uploadFilesPG(fnames []string, pool *pgx.ConnPool, nbInjectors uint) {
	fnamesChan := make(chan string)
	var wg sync.WaitGroup

	for i := uint(0); i < nbInjectors; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				fname, ok := <-fnamesChan
				if !ok {
					return
				}
				uploadFilePG(fname, pool)
			}
		}()
	}

	for _, fname := range fnames {
		fnamesChan <- fname
	}
	close(fnamesChan)
	wg.Wait()
}

func uploadFilePG(fname string, pool *pgx.ConnPool) {
	fname = strings.TrimSpace(fname)
	f, err := os.Open(fname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening '%s': %s\n", fname, err)
		return
	}

	err = uploadPG(f, pool)
	f.Close()
	if err == nil {
		fmt.Fprintln(os.Stderr, "Successfully uploaded:", fname)
	} else {
		fmt.Fprintf(os.Stderr, "Error uploading '%s': %s\n", fname, err)
	}
}

func uploadPG(f io.Reader, connPool *pgx.ConnPool) error {
	rows := make([][]interface{}, 0, 1000)
	var row []interface{}
	var l *parser.Line

	p := parser.NewFileParser(f)
	err := p.ParseHeader()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error building parser:", err)
		return err
	}
	fieldNames = p.FieldNames()
	if !p.HasGmtTime() {
		fieldNames = append([]string{"gmttime"}, fieldNames...)
	}
	nbFields := len(fieldNames)

	columnNames := make([]string, 0, nbFields)
	types := make(map[string]parser.Kind, nbFields)
	for _, name := range fieldNames {
		// make sure column names are PG compatible
		columnNames = append(columnNames, pgKey(name))
		// store the data type for each column
		types[name] = parser.GuessType(name)
	}

	rowPool := &sync.Pool{
		New: func() interface{} {
			return make([]interface{}, 0, nbFields)
		},
	}

	if err != nil {
		return err
	}
	for {
		row = rowPool.Get().([]interface{})[:0]
		l, err = p.NextTo(l)
		if l == nil || err != nil {
			break
		}
		for _, name := range fieldNames {
			// append converted type
			row = append(row, pgConvert(types[name], l.Get(name)))
		}
		rows = append(rows, row)
		if len(rows) == 1000 {
			_, err = connPool.CopyFrom(
				pgx.Identifier{tableName},
				columnNames,
				pgx.CopyFromRows(rows),
			)
			if err != nil {
				return err
			}
			for _, row = range rows {
				rowPool.Put(row)
			}
			rows = rows[:0]
		}
	}
	if len(rows) > 0 {
		_, err = connPool.CopyFrom(
			pgx.Identifier{tableName},
			columnNames,
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return err
		}
		for _, row = range rows {
			rowPool.Put(row)
		}
		rows = rows[:0]
	}
	_, err = connPool.Exec("VACUUM;")
	return err

}

// MyMyTime encapsulates parser.Time so that it can be serialized to PG.
//
// We don't implement EncodeBinary on MyTime to avoid a dependancy on pgio
// in the library part.
type MyMyTime struct {
	parser.Time
}

// EncodeBinary implements the MarshalBinary interface.
func (src *MyMyTime) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if src == nil {
		return nil, nil
	}
	return pgio.AppendInt64(
		buf,
		(int64(src.Hour)*usecsPerHour)+(int64(src.Minute)*usecsPerMinute)+(int64(src.Second)*usecsPerSec)+(int64(src.Nanosecond)/nanosecsPerUsec),
	), nil
}

func pgDefaultVal(t parser.Kind) interface{} {
	switch t {
	case parser.MyDate:
		return &pgtype.Date{Status: pgtype.Null}
	case parser.MyIP:
		return &pgtype.Inet{Status: pgtype.Null}
	case parser.MyTime:
		var timePtr *MyMyTime
		return timePtr
	case parser.MyTimestamp:
		return &pgtype.Timestamptz{Status: pgtype.Null}
	case parser.MyURI:
		return ""
	case parser.Float64:
		return &pgtype.Float8{Status: pgtype.Null}
	case parser.Int64:
		return &pgtype.Int8{Status: pgtype.Null}
	case parser.Bool:
		return &pgtype.Bool{Status: pgtype.Null}
	case parser.String:
		return ""
	}
	return ""
}

func pgConvert(t parser.Kind, value interface{}) interface{} {
	if value == nil {
		return pgDefaultVal(t)
	}
	switch t {
	case parser.MyDate:
		v := value.(parser.Date)
		if v.IsZero() {
			return pgDefaultVal(t)
		}
		return time.Date(v.Year, v.Month, v.Day, 0, 0, 0, 0, time.UTC)
	case parser.MyIP:
		inet := &pgtype.Inet{}
		inet.Set(value.(net.IP))
		return inet
	case parser.MyTime:
		v := value.(parser.Time)
		if v.IsZero() {
			return pgDefaultVal(t)
		}
		return &MyMyTime{Time: v}
	case parser.MyTimestamp:
		v := value.(time.Time)
		if v.IsZero() {
			return pgDefaultVal(t)
		}
		return &pgtype.Timestamptz{Status: pgtype.Present, Time: v}
	case parser.MyURI:
		return decodeCharset(value.(string))
	case parser.Float64:
		return value.(float64)
	case parser.Int64:
		return value.(int64)
	case parser.Bool:
		return value.(bool)
	case parser.String:
		return decodeCharset(value.(string))
	}
	return decodeCharset(value.(string))
}

func decodeCharset(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	utf, err := isoDecoder.String(s)
	if err == nil {
		return utf
	}
	return unidecode.Unidecode(s)
}

func init() {
	rootCmd.AddCommand(push2pgCmd)
	push2pgCmd.Flags().StringArrayVar(&fnames, "filename", []string{}, "the files to parse")
	push2pgCmd.Flags().StringVar(&tableName, "tablename", "accesslogs", "name of pg table to push events to")
	push2pgCmd.Flags().StringVar(&dbURI, "uri", "", "the URI of the postgresql server to connect to")
	push2pgCmd.Flags().Uint8Var(&parallel, "parallel", 1, "number of parallel injectors")
}

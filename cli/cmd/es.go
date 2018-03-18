package cmd

import (
	"fmt"
	"strconv"
	"time"

	"github.com/inconshreveable/log15"
	parser "github.com/stephane-martin/w3c-extendedlog-parser"
)

type esOpts struct {
	S esSettings `json:"settings"`
	M esMappings `json:"mappings"`
}

func newEsOpts(shards uint, replicas uint, checkStartup bool, refreshInterval time.Duration, fieldNames []string) esOpts {
	return esOpts{
		S: newSettings(shards, replicas, checkStartup, refreshInterval),
		M: newMappings(fieldNames),
	}
}

type esSettings struct {
	Shards   uint          `json:"number_of_shards"`
	Replicas uint          `json:"number_of_replicas"`
	Shard    shardSettings `json:"shard"`
	Refresh  string        `json:"refresh_interval"`
}

type shardSettings struct {
	Check bool `json:"check_on_startup"`
}

func newSettings(s uint, r uint, c bool, refr time.Duration) esSettings {
	return esSettings{
		Shards:   s,
		Replicas: r,
		Shard:    shardSettings{Check: c},
		Refresh:  strconv.FormatInt(int64(refr.Seconds()), 10) + "s",
	}
}

type esMappings struct {
	Mtyp esType `json:"accesslogs"`
}

func newMappings(fieldNames []string) esMappings {
	return esMappings{
		Mtyp: esType{
			Properties: newMessageFields(fieldNames),
		},
	}
}

type esType struct {
	Properties esFields `json:"properties"`
}

type esFields map[string]anyEsField

func newMessageFields(fieldNames []string) (fields esFields) {
	fields = make(map[string]anyEsField)
	for _, name := range fieldNames {
		switch parser.GuessType(name) {
		case parser.MyDate:
			fields[name] = newDateField()
		case parser.MyIP:
			fields[name] = newIPField()
		case parser.MyTime:
			fields[name] = newTimeField()
		case parser.MyTimestamp:
			fields[name] = newDatetimeField()
		case parser.MyURI:
			fields[name] = newKeyword()
		case parser.Float64:
			fields[name] = newDoubleField()
		case parser.Int64:
			fields[name] = newLongField()
		case parser.Bool:
			fields[name] = newBoolField()
		case parser.String:
			fields[name] = newMulti()
		default:
			fields[name] = newMulti()
		}
	}
	fields["@timestamp"] = newDatetimeField()
	fields["fulltext"] = newTextField()
	return fields
}

type anyEsField interface{}

type doubleEsField struct {
	Typ   string `json:"type"`
	Store bool   `json:"store"`
}

func newDoubleField() doubleEsField {
	return doubleEsField{
		Typ:   "double",
		Store: true,
	}
}

type boolEsField struct {
	Typ   string `json:"type"`
	Store bool   `json:"store"`
}

func newBoolField() boolEsField {
	return boolEsField{
		Typ:   "boolean",
		Store: true,
	}
}

type ipEsField struct {
	Typ   string `json:"type"`
	Store bool   `json:"store"`
}

func newIPField() ipEsField {
	return ipEsField{
		Typ:   "ip",
		Store: true,
	}
}

type strMultiEsField struct {
	Typ    string     `json:"type"`
	Store  bool       `json:"store"`
	Fields rawEsField `json:"fields"`
	Copy   string     `json:"copy_to"`
}

type rawEsField struct {
	Raw rawRawEsField `json:"raw"`
}

type rawRawEsField struct {
	Typ string `json:"type"`
}

func newMulti() strMultiEsField {
	return strMultiEsField{
		Typ:   "text",
		Store: true,
		Copy:  "fulltext",
		Fields: rawEsField{
			Raw: rawRawEsField{
				Typ: "keyword",
			},
		},
	}
}

type strEsField struct {
	Typ   string `json:"type"`
	Store bool   `json:"store"`
}

func newKeyword() strEsField {
	return strEsField{
		Typ:   "keyword",
		Store: true,
	}
}

func newTextField() strEsField {
	return strEsField{
		Typ:   "text",
		Store: true,
	}
}

type dateEsField struct {
	Typ    string `json:"type"`
	Format string `json:"format"`
	Store  bool   `json:"store"`
}

func newDateField() dateEsField {
	return dateEsField{
		Typ:    "date",
		Format: "strict_date",
		Store:  true,
	}
}

type timeEsField struct {
	Typ    string `json:"type"`
	Format string `json:"format"`
	Store  bool   `json:"store"`
}

func newTimeField() timeEsField {
	return timeEsField{
		Typ:    "date",
		Format: "strict_time_no_millis||strict_time||strict_hour_minute_second||strict_hour_minute_second_fraction",
		Store:  true,
	}
}

type datetimeEsField struct {
	Typ    string `json:"type"`
	Format string `json:"format"`
	Store  bool   `json:"store"`
}

func newDatetimeField() datetimeEsField {
	return datetimeEsField{
		Typ:    "date",
		Format: "strict_date_time_no_millis||strict_date_time",
		Store:  true,
	}
}

type longEsField struct {
	Typ   string `json:"type"`
	Store bool   `json:"store"`
}

func newLongField() longEsField {
	return longEsField{
		Typ:   "long",
		Store: true,
	}
}

type esLogger struct {
	Logger log15.Logger
}

func (l *esLogger) Printf(format string, v ...interface{}) {
	l.Logger.Info(fmt.Sprintf(format, v...))
}

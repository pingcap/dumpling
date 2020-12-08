// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"github.com/pingcap/errors"

	"github.com/pingcap/dumpling/v4/log"
)

const (
	outputFileTemplateSchema = "schema"
	outputFileTemplateTable  = "table"
	outputFileTemplateView   = "view"
	outputFileTemplateData   = "data"

	defaultOutputFileTemplateBase = `
		{{- define "objectName" -}}
			{{fn .DB}}.{{fn .Table}}
		{{- end -}}
		{{- define "schema" -}}
			{{fn .DB}}-schema-create
		{{- end -}}
		{{- define "event" -}}
			{{template "objectName" .}}-schema-post
		{{- end -}}
		{{- define "function" -}}
			{{template "objectName" .}}-schema-post
		{{- end -}}
		{{- define "procedure" -}}
			{{template "objectName" .}}-schema-post
		{{- end -}}
		{{- define "sequence" -}}
			{{template "objectName" .}}-schema-sequence
		{{- end -}}
		{{- define "trigger" -}}
			{{template "objectName" .}}-schema-triggers
		{{- end -}}
		{{- define "view" -}}
			{{template "objectName" .}}-schema-view
		{{- end -}}
		{{- define "table" -}}
			{{template "objectName" .}}-schema
		{{- end -}}
		{{- define "data" -}}
			{{template "objectName" .}}.{{.Index}}
		{{- end -}}
	`

	// DefaultAnonymousOutputFileTemplateText is the default anonymous output file templateText for dumpling's table data file name
	DefaultAnonymousOutputFileTemplateText = "result.{{.Index}}"
)

var (
	filenameEscapeRegexp = regexp.MustCompile(`[\x00-\x1f%"*./:<>?\\|]|-(?i:schema)`)
	// DefaultOutputFileTemplate is the default output file template for dumpling's table data file name
	DefaultOutputFileTemplate = template.Must(template.New("data").
					Option("missingkey=error").
					Funcs(template.FuncMap{
			"fn": func(input string) string {
				return filenameEscapeRegexp.ReplaceAllStringFunc(input, func(match string) string {
					return fmt.Sprintf("%%%02X%s", match[0], match[1:])
				})
			},
		}).
		Parse(defaultOutputFileTemplateBase))
)

// ParseOutputFileTemplate parses template from the specified text
func ParseOutputFileTemplate(text string) (*template.Template, error) {
	return template.Must(DefaultOutputFileTemplate.Clone()).Parse(text)
}

func prepareDumpingDatabases(conf *Config, db *sql.Conn) ([]string, error) {
	databases, err := ShowDatabases(db)
	if len(conf.Databases) == 0 {
		return databases, err
	}
	dbMap := make(map[string]interface{}, len(databases))
	for _, database := range databases {
		dbMap[database] = struct{}{}
	}
	var notExistsDatabases []string
	for _, database := range conf.Databases {
		if _, ok := dbMap[database]; !ok {
			notExistsDatabases = append(notExistsDatabases, database)
		}
	}
	if len(notExistsDatabases) > 0 {
		return nil, errors.Errorf("Unknown databases [%s]", strings.Join(notExistsDatabases, ","))
	}
	return conf.Databases, nil
}

func listAllTables(db *sql.Conn, databaseNames []string) (DatabaseTables, error) {
	log.Debug("list all the tables")
	return ListAllDatabasesTables(db, databaseNames, TableTypeBase)
}

func listAllViews(db *sql.Conn, databaseNames []string) (DatabaseTables, error) {
	log.Debug("list all the views")
	return ListAllDatabasesTables(db, databaseNames, TableTypeView)
}

type databaseName = string

// TableType represents the type of table
type TableType int8

const (
	// TableTypeBase represents the basic table
	TableTypeBase TableType = iota
	// TableTypeView represents the view table
	TableTypeView
)

// TableInfo is the table info for a table in database
type TableInfo struct {
	Name string
	Type TableType
}

// Equals returns true the table info is the same with another one
func (t *TableInfo) Equals(other *TableInfo) bool {
	return t.Name == other.Name && t.Type == other.Type
}

// DatabaseTables is the type that represents tables in a database
type DatabaseTables map[databaseName][]*TableInfo

// NewDatabaseTables returns a new DatabaseTables
func NewDatabaseTables() DatabaseTables {
	return DatabaseTables{}
}

// AppendTable appends a TableInfo to DatabaseTables
func (d DatabaseTables) AppendTable(dbName string, table *TableInfo) DatabaseTables {
	d[dbName] = append(d[dbName], table)
	return d
}

// AppendTables appends several basic tables to DatabaseTables
func (d DatabaseTables) AppendTables(dbName string, tableNames ...string) DatabaseTables {
	for _, t := range tableNames {
		d[dbName] = append(d[dbName], &TableInfo{t, TableTypeBase})
	}
	return d
}

// AppendViews appends several views to DatabaseTables
func (d DatabaseTables) AppendViews(dbName string, viewNames ...string) DatabaseTables {
	for _, v := range viewNames {
		d[dbName] = append(d[dbName], &TableInfo{v, TableTypeView})
	}
	return d
}

// Merge merges another DatabaseTables
func (d DatabaseTables) Merge(other DatabaseTables) {
	for name, infos := range other {
		d[name] = append(d[name], infos...)
	}
}

// Literal returns a user-friendly output for DatabaseTables
func (d DatabaseTables) Literal() string {
	var b strings.Builder
	b.WriteString("tables list\n")
	b.WriteString("\n")

	for dbName, tables := range d {
		b.WriteString("schema ")
		b.WriteString(dbName)
		b.WriteString(" :[")
		for _, tbl := range tables {
			b.WriteString(tbl.Name)
			b.WriteString(", ")
		}
		b.WriteString("]")
	}

	return b.String()
}

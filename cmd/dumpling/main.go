// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"errors"
	"fmt"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/dumpling/v4/cli"
	"github.com/pingcap/dumpling/v4/export"
	"github.com/pingcap/log"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

var (
	databases     []string
	tablesList    []string
	host          string
	user          string
	port          int
	password      string
	threads       int
	outputDir     string
	fileSizeStr   string
	statementSize uint64
	logLevel      string
	logFile       string
	logFormat     string
	consistency   string
	snapshot      string
	noViews       bool
	statusAddr    string
	rows          uint64
	where         string
	fileType      string
	noHeader      bool
	noSchemas     bool
	noData        bool
	csvNullValue  string
	sql           string
	filters       []string
	caseSensitive bool

	escapeBackslash bool
)

var defaultOutputDir = timestampDirName()

func timestampDirName() string {
	return fmt.Sprintf("./export-%s", time.Now().Format(time.RFC3339))
}

func main() {
	pflag.Usage = func() {
		fmt.Fprint(os.Stderr, "Dumpling is a CLI tool that helps you dump MySQL/TiDB data\n\nUsage:\n  dumpling [flags]\n\nFlags:\n")
		pflag.PrintDefaults()
	}
	pflag.ErrHelp = errors.New("")

	pflag.StringSliceVarP(&databases, "database", "B", nil, "Databases to dump")
	pflag.StringSliceVarP(&tablesList, "tables-list", "T", nil, "Comma delimited table list to dump; must be qualified table names")
	pflag.StringVarP(&host, "host", "h", "127.0.0.1", "The host to connect to")
	pflag.StringVarP(&user, "user", "u", "root", "Username with privileges to run the dump")
	pflag.IntVarP(&port, "port", "P", 4000, "TCP/IP port to connect to")
	pflag.StringVarP(&password, "password", "p", "", "User password")
	pflag.IntVarP(&threads, "threads", "t", 4, "Number of goroutines to use, default 4")
	pflag.StringVarP(&fileSizeStr, "filesize", "F", "", "The approximate size of output file")
	pflag.Uint64VarP(&statementSize, "statement-size", "s", export.UnspecifiedSize, "Attempted size of INSERT statement in bytes")
	pflag.StringVarP(&outputDir, "output", "o", defaultOutputDir, "Output directory")
	pflag.StringVar(&logLevel, "loglevel", "info", "Log level: {debug|info|warn|error|dpanic|panic|fatal}")
	pflag.StringVarP(&logFile, "logfile", "L", "", "Log file `path`, leave empty to write to console")
	pflag.StringVar(&logFormat, "logfmt", "text", "Log `format`: {text|json}")
	pflag.StringVar(&consistency, "consistency", "auto", "Consistency level during dumping: {auto|none|flush|lock|snapshot}")
	pflag.StringVar(&snapshot, "snapshot", "", "Snapshot position. Valid only when consistency=snapshot")
	pflag.BoolVarP(&noViews, "no-views", "W", true, "Do not dump views")
	pflag.StringVar(&statusAddr, "status-addr", ":8281", "dumpling API server and pprof addr")
	pflag.Uint64VarP(&rows, "rows", "r", export.UnspecifiedSize, "Split table into chunks of this many rows, default unlimited")
	pflag.StringVar(&where, "where", "", "Dump only selected records")
	pflag.BoolVar(&escapeBackslash, "escape-backslash", true, "use backslash to escape quotation marks")
	pflag.StringVar(&fileType, "filetype", "sql", "The type of export file (sql/csv)")
	pflag.BoolVar(&noHeader, "no-header", false, "whether not to dump CSV table header")
	pflag.BoolVarP(&noSchemas, "no-schemas", "m", false, "Do not dump table schemas with the data")
	pflag.BoolVarP(&noData, "no-data", "d", false, "Do not dump table data")
	pflag.StringVar(&csvNullValue, "csv-null-value", "\\N", "The null value used when export to csv")
	pflag.StringVarP(&sql, "sql", "S", "", "Dump data with given sql")
	pflag.StringArrayVarP(&filters, "filter", "f", []string{"*.*"}, "filter to select which tables to dump")
	pflag.BoolVar(&caseSensitive, "case-sensitive", false, "whether the filter should be case-sensitive")

	printVersion := pflag.BoolP("version", "V", false, "Print Dumpling version")

	pflag.Parse()

	println(cli.LongVersion())

	if *printVersion {
		return
	}

	tableFilter, err := parseTableFilter()
	if err != nil {
		fmt.Printf("failed to parse filter: %s\n", err)
		os.Exit(2)
	}
	if !caseSensitive {
		tableFilter = filter.CaseInsensitive(tableFilter)
	}

	var fileSize uint64
	if len(fileSizeStr) == 0 {
		fileSize = export.UnspecifiedSize
	} else if fileSizeMB, err := strconv.ParseUint(fileSizeStr, 10, 64); err == nil {
		fmt.Printf("Warning: -F without unit is not recommended, try using `-F '%dMiB'` in the future\n", fileSizeMB)
		fileSize = fileSizeMB * units.MiB
	} else if size, err := units.RAMInBytes(fileSizeStr); err == nil {
		fileSize = uint64(size)
	} else {
		fmt.Printf("failed to parse filesize (-F '%s')\n", fileSizeStr)
		os.Exit(2)
	}

	conf := export.DefaultConfig()
	conf.Databases = databases
	conf.Host = host
	conf.User = user
	conf.Port = port
	conf.Password = password
	conf.Threads = threads
	conf.FileSize = fileSize
	conf.StatementSize = statementSize
	conf.OutputDirPath = outputDir
	conf.Consistency = consistency
	conf.NoViews = noViews
	conf.StatusAddr = statusAddr
	conf.Rows = rows
	conf.Where = where
	conf.EscapeBackslash = escapeBackslash
	conf.LogLevel = logLevel
	conf.LogFile = logFile
	conf.LogFormat = logFormat
	conf.FileType = fileType
	conf.NoHeader = noHeader
	conf.NoSchemas = noSchemas
	conf.NoData = noData
	conf.CsvNullValue = csvNullValue
	conf.Sql = sql
	conf.TableFilter = tableFilter

	err = export.Dump(conf)
	if err != nil {
		log.Error("dump failed error stack info", zap.Error(err))
		fmt.Printf("\ndump failed: %s\n", err.Error())
		os.Exit(1)
	}
}

func parseTableFilter() (filter.Filter, error) {
	if len(tablesList) == 0 {
		return filter.Parse(filters)
	}

	// only parse -T when -f is default value. otherwise bail out.
	if len(filters) != 1 || filters[0] != "*.*" {
		return nil, errors.New("cannot pass --tables-list and --filter together")
	}

	tableNames := make([]filter.Table, 0, len(tablesList))
	for _, table := range tablesList {
		parts := strings.SplitN(table, ".", 2)
		if len(parts) < 2 {
			return nil, fmt.Errorf("--tables-list only accepts qualified table names, but `%s` lacks a dot", table)
		}
		tableNames = append(tableNames, filter.Table{Schema: parts[0], Name: parts[1]})
	}

	return filter.NewTablesFilter(tableNames...), nil
}

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
	"fmt"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/pingcap/dumpling/v4/cli"
	"github.com/pingcap/dumpling/v4/export"
	"github.com/pingcap/dumpling/v4/log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	database    string
	host        string
	user        string
	port        int
	password    string
	threads     int
	outputDir   string
	fileSize    uint64
	logLevel    string
	consistency string
	snapshot    string
	noViews     bool

	rootCmd = &cobra.Command{
		Use:   "dumpling",
		Short: "A tool to dump MySQL/TiDB data",
		Long:  `Dumpling is a CLI tool that helps you dump MySQL/TiDB data`,
		Run: func(cmd *cobra.Command, args []string) {
			run()
		},
	}
)

var defaultOutputDir = timestampDirName()

func timestampDirName() string {
	return fmt.Sprintf("./export-%s", time.Now().Format(time.RFC3339))
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&database, "database", "B", "", "Database to dump")
	rootCmd.PersistentFlags().StringVarP(&host, "host", "H", "127.0.0.1", "The host to connect to")
	rootCmd.PersistentFlags().StringVarP(&user, "user", "u", "root", "Username with privileges to run the dump")
	rootCmd.PersistentFlags().IntVarP(&port, "port", "P", 4000, "TCP/IP port to connect to")
	rootCmd.PersistentFlags().StringVarP(&password, "password", "p", "", "User password")
	rootCmd.PersistentFlags().IntVarP(&threads, "threads", "t", 4, "Number of goroutines to use, default 4")
	rootCmd.PersistentFlags().Uint64VarP(&fileSize, "filesize", "F", export.UnspecifiedSize, "The approximate size of output file")
	rootCmd.PersistentFlags().StringVarP(&outputDir, "output", "o", defaultOutputDir, "Output directory")
	rootCmd.PersistentFlags().StringVar(&logLevel, "loglevel", "info", "Log level: {debug|info|warn|error|dpanic|panic|fatal}")
	rootCmd.PersistentFlags().StringVar(&consistency, "consistency", "auto", "Consistency level during dumping: {auto|none|flush|lock|snapshot}")
	rootCmd.PersistentFlags().StringVar(&snapshot, "snapshot", "", "Snapshot position. Valid only when consistency=snapshot")
	rootCmd.PersistentFlags().BoolVarP(&noViews, "no-views", "W", true, "Do not dump views")
}

func run() {
	println(cli.LongVersion())

	err := log.InitAppLogger(&log.Config{Level: logLevel})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "initialize logger failed: %s", err.Error())
		os.Exit(1)
	}

	conf := export.DefaultConfig()
	conf.Database = database
	conf.Host = host
	conf.User = user
	conf.Port = port
	conf.Password = password
	conf.Threads = threads
	conf.FileSize = fileSize
	conf.OutputDirPath = outputDir
	conf.NoViews = noViews

	err = export.Dump(conf)
	if err != nil {
		log.Zap().Error("dump failed", zap.Error(err))
		os.Exit(1)
	}
	return
}

func main() {
	rootCmd.Execute()
}

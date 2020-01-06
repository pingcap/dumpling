package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"

	"github.com/pingcap/dumpling/cmd/integration_test/naughty_strings"
	"github.com/pingcap/dumpling/v4/export"
	"github.com/pingcap/errors"
)

const (
	testDatabaseName = "test_dumpling"
	testHost         = "127.0.0.1"
	testPort         = 3306
	testPassword     = ""
)

type TestRunner interface {
	RelativeTestDataPath() string
	RelativeTestResultPath() string
	BuildConfig() *export.Config
	Prepare(dataFilePath string, db *sql.DB) error
}

var integrationTestDir string

func init() {
	wd, err := os.Getwd()
	assertNotNil(err)
	flag.StringVar(&integrationTestDir, "src", wd, "the path of directory that contains test data")
}

func main() {
	flag.Parse()
	allTestRunners := []TestRunner{
		naughty_strings.NewNaughtyStringTestRunner(),
	}

	for _, runner := range allTestRunners {
		testDataPath := path.Join(integrationTestDir, runner.RelativeTestDataPath())
		resultPath := path.Join(integrationTestDir, runner.RelativeTestResultPath())

		conf := runner.BuildConfig()
		processConfig(conf)
		dbForPrepare := setupTestDB(conf)
		err := runner.Prepare(testDataPath, dbForPrepare)
		assertNotNil(err)

		dumpResultPath := dump(conf)

		assert(dumpResultPath, resultPath)
	}
}

func setupTestDB(conf *export.Config) *sql.DB {
	db, err := sql.Open("mysql", conf.GetDSN(""))
	assertNotNil(err)
	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDatabaseName))
	assertNotNil(err)
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", testDatabaseName))
	assertNotNil(err)
	err = export.UseDatabase(db, testDatabaseName)
	assertNotNil(err)
	return db
}

func processConfig(conf *export.Config) {
	tempOutputDir := path.Join(os.TempDir(), "test-dumpling")
	err := os.RemoveAll(tempOutputDir)
	assertNotNil(err)
	conf.OutputDirPath = tempOutputDir
	conf.Database = testDatabaseName
	conf.Host = testHost
	conf.Port = testPort
	conf.Password = testPassword
}

func dump(conf *export.Config) string {
	err := export.Dump(conf)
	assertNotNil(err)

	dumpFiles, err := collectAllSQLFilePath(conf.OutputDirPath)
	assertNotNil(err)
	mergedFileName := path.Join(conf.OutputDirPath, "dumpling-merged.txt")
	mergeFile, err := os.OpenFile(mergedFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	assertNotNil(err)

	for _, filePath := range dumpFiles {
		f, err := os.OpenFile(filePath, os.O_RDWR, 0644)
		assertNotNil(err)
		_, err = io.Copy(mergeFile, f)
		assertNotNil(err)
		assertNotNil(f.Close())
	}
	assertNotNil(mergeFile.Close())
	return mergedFileName
}

func collectAllSQLFilePath(dir string) ([]string, error) {
	var result []string
	dumpFiles, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, f := range dumpFiles {
		if path.Ext(f.Name()) != ".sql" {
			continue
		}
		result = append(result, path.Join(dir, f.Name()))
	}
	return result, nil
}

func assert(obtainResultPath, expectedResultPath string) {
	obtainReader, err := openFileAsReader(obtainResultPath)
	assertNotNil(err)
	expectedReader, err := openFileAsReader(expectedResultPath)
	assertNotNil(err)

	diff := compare(obtainReader, expectedReader)
	if diff != nil {
		log.Fatal(diff.String())
	}
}

func openFileAsReader(path string) (*bufio.Reader, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return bufio.NewReader(f), nil
}

type Diff struct {
	left     string
	right    string
	lineNum  int
	leftErr  error
	rightErr error
}

func (d *Diff) String() string {
	var left, right string
	if d.leftErr != nil {
		left = fmt.Sprintf("error: %v", d.leftErr)
	} else {
		left = fmt.Sprintf("string: %s", d.left)
	}
	if d.rightErr != nil {
		right = fmt.Sprintf("error: %v", d.rightErr)
	} else {
		right = fmt.Sprintf("string: %s", d.right)
	}
	return fmt.Sprintf("left '%s', right '%s'", escapeEscape(left), escapeEscape(right))
}

func compare(obtainReader, expectedReader *bufio.Reader) *Diff {
	diff := &Diff{}
	for {
		left, leftErr := obtainReader.ReadString('\n')
		right, rightErr := expectedReader.ReadString('\n')
		switch {
		case leftErr == io.EOF && rightErr == io.EOF:
			return nil
		case leftErr == nil && rightErr == nil:
			if left == right {
				diff.lineNum += 1
			} else {
				diff.left, diff.right = left, right
				return diff
			}
		default:
			diff.left, diff.right = left, right
			diff.leftErr, diff.rightErr = leftErr, rightErr
			return diff
		}
	}
}

func escapeEscape(src string) string {
	return strings.ReplaceAll(src, "\n", `\n`)
}

func assertNotNil(err error) {
	if err != nil {
		log.Fatal(errors.WithStack(err))
	}
}

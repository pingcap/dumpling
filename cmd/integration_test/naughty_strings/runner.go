package naughty_strings

import (
	"database/sql"
	"encoding/json"
	"github.com/pingcap/dumpling/v4/export"
	"io/ioutil"
	"path"
)

type NaughtyStringTestRunner struct {
}

func (n *NaughtyStringTestRunner) BuildConfig() *export.Config {
	conf := export.DefaultConfig()
	conf.Snapshot = ""
	conf.SortByPk = false
	conf.FileSize = export.UnspecifiedSize

	return conf
}

func (n *NaughtyStringTestRunner) Prepare(dataFilePath string, db *sql.DB) error {
	data, err := ioutil.ReadFile(dataFilePath)
	if err != nil {
		return err
	}

	var strs []string
	if err = json.Unmarshal(data, &strs); err != nil {
		return err
	}

	_, err = db.Exec("CREATE TABLE t (a TEXT(65535))")
	if err != nil {
		return err
	}
	for _, str := range strs {
		_, err = db.Exec("INSERT INTO t values (?)", str)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *NaughtyStringTestRunner) RelativeTestDataPath() string {
	return path.Join("naughty_strings", "base64_data.json")
}

func (n *NaughtyStringTestRunner) RelativeTestResultPath() string {
	return path.Join("naughty_strings", "result.sql")
}

func NewNaughtyStringTestRunner() *NaughtyStringTestRunner {
	return &NaughtyStringTestRunner{}
}

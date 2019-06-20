package main

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"io/ioutil"
	"os"
	"strings"

	"github.com/joho/sqltocsv"
)

func ZipFiles(filedir string, filename string, rows *sql.Rows) string {
	_ = sqltocsv.WriteFile(filedir+filename, rows)
	f, _ := os.Open(filedir + filename)
	reader := bufio.NewReader(f)
	content, _ := ioutil.ReadAll(reader)

	filename = strings.Split(filename, ".csv")[0] + ".gz"

	newZipFile, _ := os.Create(filedir + filename)
	defer newZipFile.Close()

	zipWriter := gzip.NewWriter(newZipFile)
	defer zipWriter.Close()

	zipWriter.Write([]byte(content))

	return filename
}

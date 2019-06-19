package main

import (
	"archive/zip"
	"database/sql"
	"io"
	"os"
	"strings"

	"github.com/joho/sqltocsv"
)

func ZipFiles(filedir string, filename string, rows *sql.Rows) string {
	files := filename
	_ = sqltocsv.WriteFile(filedir+filename, rows)
	filename = strings.Split(filename, ".csv")[0] + ".zip"
	newZipFile, _ := os.Create(filedir + filename)
	defer newZipFile.Close()

	zipWriter := zip.NewWriter(newZipFile)
	defer zipWriter.Close()

	// Add files to zip
	_ = AddFileToZip(zipWriter, filedir+files)
	return filename
}

func AddFileToZip(zipWriter *zip.Writer, filename string) error {
	fileToZip, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fileToZip.Close()

	// Get the file information
	info, err := fileToZip.Stat()
	if err != nil {
		return err
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
	}

	// Using FileInfoHeader() above only uses the basename of the file. If we want
	// to preserve the folder structure we can overwrite this with the full path.
	header.Name = filename

	// Change to deflate to gain better compression
	// see http://golang.org/pkg/archive/zip/#pkg-constants
	header.Method = zip.Deflate

	header.Flags = 0x800 //UTF-8 because AWS GLUE just only zip with using UTF-8 decoding

	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		return err
	}
	_, err = io.Copy(writer, fileToZip)
	return err
}

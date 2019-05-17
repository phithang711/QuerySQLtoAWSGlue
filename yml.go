package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql" // or the driver of your choice
	"github.com/joho/sqltocsv"

	"github.com/robfig/cron"

	"bytes"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/viper"
)

type Configuration struct {
	Databases map[string]database `yaml:"databases"`
	Storages  map[string]storage  `yaml:"storages"`
	Exporters []exporter `yaml:"exporters"`
}

type database struct {
	Dbtype     string `yaml:"dbtype"`
	Dbuser     string `yaml:"dbuser"`
	Dbpassword string `yaml:"dbpassword"`
	Dbip       string `yaml:"dbip"`
	Dbport     string `yaml:"dbport"`
	DbName     string `yaml:"dbName"`
}

type storage struct {
	S3bucket string `yaml:"s3bucket"`
	S3region string `yaml:"s3region"`
}

type exporter struct {
	Scheduler 		string `yaml:"scheduler"`
	Query    	 	string `yaml:"query"`
	Querykey  		string `yaml:"querykey"`
	Database  		string `yaml:"database"`
	Storage   		string `yaml:"storage"`
	Subfolder 		string `yaml:"subfolder"`
	Subfolderinaws  string `yaml:"subfolderinaws"`
	Filename  		string `yaml:"filename"`
}

func main() {
	a := ParseConfig()
	a.PrepareCron()
}

func (a Configuration) PrepareCron() {
	report := ReadKeyFileData()

	var i int
	for i=range a.Exporters{}
	key:=make([]int,i+1)
	
	c := cron.New()
	for i = range a.Exporters {
		a.CheckKeyDataAndAddToCron(c, i, key, report)
	}

	// start cron
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
    go func() {
		<-sig
		c.Stop()
        fmt.Println("\nPause and exit")
        os.Exit(1)
	}()

	for	{ c.Start() }
}

func (a Configuration) CheckKeyDataAndAddToCron(c *cron.Cron, index int, key []int, report [][]string){
	//check if key[index] of every single exporters has or not
	key[index] = 0
	if report!=nil {
		key[index], _ = strconv.Atoi(report[index][0])
		fmt.Println("report[", index, "]= ", report[index][0])
	}

	a.AddDataEverySingleScheduleIntoCron(c, key, index)	
}

func (a Configuration) AddDataEverySingleScheduleIntoCron(c *cron.Cron, key []int, index int){
	// input given input in config into cron
	c.AddFunc(a.Exporters[index].Scheduler, func() {
	func(i int) {
		key[i] = a.Exporters[i].StartAddData(a.Databases[a.Exporters[i].Database], a.Storages[a.Exporters[i].Storage], key[i])
		st := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(key)), "\n"), "[]")
		WriteToFile(st)
		fmt.Println(key)
	}(index)
	})
}

func ReadKeyFileData() ([][]string){
	var report [][]string
	csvfile, err := os.Open("key.csv")
	if err == nil {
		r := csv.NewReader(csvfile)
		report, _ = r.ReadAll()
	}
    csvfile.Close()
	return report
}


func WriteToFile(st string) {
	file, err := os.Create("key.csv")
	if err != nil {
		fmt.Println("Can not create file")
	}
	defer file.Close()
	csvWriter := bufio.NewWriter(file)
	csvWriter.WriteString(st)
	csvWriter.Flush()
}


func (export exporter) StartAddData(database database, storage storage, key int) (int) {
	fmt.Println(key)

	check := strings.Contains(export.Query, "WHERE")

	db, err := sql.Open(database.Dbtype, database.Dbuser+":"+database.Dbpassword+"@tcp("+database.Dbip+":"+database.Dbport+")/"+database.DbName)

	if err != nil {
		panic(err)
	}
	
	defer db.Close()
	

    //take the key rows in the file and take the end int element of the rows and save the element to key
	newkey, checkifchange := export.NewKeyIndex(db, key, check)

	//query with the given query sentence in yaml
	export.StartQueryDB(db,key,check, storage, checkifchange)	

	if newkey>key {
		return newkey
	} else {
		return key
	}
}

func (export exporter) StartQueryDB(db *sql.DB, key int, check bool, storageuptos3 storage, checkifchange bool){
	var rows *sql.Rows
	if check == true {
		rows, _ = db.Query(export.Query + " AND ( " + export.Querykey + " > " + strconv.Itoa(key) + ")")
	} else {
		rows, _ = db.Query(export.Query + " WHERE ( " + export.Querykey + " > " + strconv.Itoa(key) + ")")
	}

	filename := export.ChangeFilenameIfChangeKeyIndex(checkifchange)
	err := sqltocsv.WriteFile(export.Subfolder+filename, rows)
	storageuptos3.CheckS3IfAvailable(filename, export)

	if err != nil {
		panic(err)
	}
}

func (export exporter) NewKeyIndex(db *sql.DB,key int, check bool)(int,bool){
	var newkey int
	var checkifchange bool
	var rows *sql.Rows

	if check == true {
		rows, _ = db.Query("SELECT " + export.Querykey + " FROM " + strings.Split(export.Query, "FROM")[1] + " AND ( " + export.Querykey + " > " + strconv.Itoa(key) + ")")
	} else {
		rows, _ = db.Query("SELECT " + export.Querykey + " FROM " + strings.Split(export.Query, "FROM")[1] + " WHERE ( " + export.Querykey + " > " + strconv.Itoa(key) + ")")
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&newkey)
		if err != nil {
			log.Fatal(err)
		}
		err = rows.Err()
		if err != nil {
			log.Fatal(err)
		}
	}

	if (newkey>key) && (key!=0){
		checkifchange = true
	} else {
		checkifchange = false
	}

	return newkey, checkifchange
}

func (export exporter) ChangeFilenameIfChangeKeyIndex(check bool) (string){
	var filename string
	dt := time.Now()
	if check==true {
		filename = export.Filename + dt.Format("_20060201_150405") + ".csv"
	} else {
		filename = export.Filename + ".csv"
	}
	return filename
}

func (store storage) CheckS3IfAvailable(fileName string, export exporter) {
	s, testconnect := session.NewSession(&aws.Config{Region: aws.String(store.S3region)})
	if testconnect != nil {
		log.Fatalf("cannot to s3 data: %v", testconnect)
	}
	testconnect = store.AddFileToS3(s, fileName, export)
}

func (store storage) AddFileToS3(s *session.Session, fileDir string, export exporter) error {
	file, err := os.Open(export.Subfolder+fileDir)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get file size and read the file content into a buffer
	fileInfo, _ := file.Stat()
	var size int64 = fileInfo.Size()
	buffer := make([]byte, size)
	file.Read(buffer)

	// Config settings: this is where you choose the bucket, filename, content-type etc.
	// of the file you're uploading.
	_, err = s3.New(s).PutObject(&s3.PutObjectInput{
		Bucket:               aws.String(store.S3bucket),
		Key:                  aws.String(export.Subfolderinaws+ fileDir),
		ACL:                  aws.String("private"),
		Body:                 bytes.NewReader(buffer),
		ContentLength:        aws.Int64(size),
		ContentType:          aws.String(http.DetectContentType(buffer)),
		ContentDisposition:   aws.String("attachment"),
		ServerSideEncryption: aws.String("AES256"),
	})
	return err
}

func ParseConfig() Configuration {
	cfg := viper.New()
	cfg.SetConfigName("config")
	cfg.AddConfigPath(".") // optionally look for config in the working directory

	err := cfg.ReadInConfig() // Find and read the config file
	if err != nil {           // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s", err))
	}

	var config Configuration

	err = cfg.Unmarshal(&config)
	if err != nil {
		panic("Unable to unmarshal config")
	}

	return config
}
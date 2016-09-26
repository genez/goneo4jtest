package main

import (
	"compress/gzip"
	"database/sql"
	"encoding/base32"
	"encoding/csv"
	"flag"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/jmoiron/sqlx"
	"log"
	"os"
	"strconv"
	"time"
)

var c = make([]byte, 5)
var digits = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

func getEncodedTimeStamp() string {
	time.Sleep(50 * time.Millisecond)
	l := uint64(time.Now().UnixNano() / 1000 / 1000 / 20)
	c[0] = digits[l&63]
	l = l >> 6
	c[1] = digits[l&63]
	l = l >> 6
	c[2] = digits[l&63]
	l = l >> 6
	c[3] = digits[l&63]
	l = l >> 6
	c[4] = digits[l&63]
	return base32.StdEncoding.EncodeToString(c)
}

var ntins = make(map[int64]string)

func main() {

	server := flag.String("server", "83.136.250.39\\SQL2012", "server name")
	db, err := sqlx.Open("mssql", fmt.Sprintf("server=%s;database=AntaresTrackingBenchmark;user id=tav;password=tav", *server))
	err = db.Ping()
	checkError("cannot open mssql", err)

	log.Println("Exporting NTIN")
	exportNtins(db)
	log.Println("done")

	log.Println("Exporting LOT")
	exportLots(db)
	log.Println("done")

	var totalItems int
	var lots int
	err = db.QueryRow("select count(*), count(distinct WorkOrderid) from dbo.Item").Scan(&totalItems, &lots)
	checkError("query failed", err)

	log.Println("Exporting ", totalItems, " total items in ", lots, " work orders")
	exportItems(db)
	log.Println("done")
}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}

// Item represents an item (meh)
type Item struct {
	NtinID       int64          `db:"NtinId"`
	Serial       string         `db:"Serial"`
	Status       int64          `db:"Status"`
	ParentNtinID sql.NullInt64  `db:"ParentNtinId"`
	ParentSerial sql.NullString `db:"ParentSerial"`
	WorkOrderID  string         `db:"WorkOrderID"`
	Sequence     int64          `db:"Sequence"`
	Type         int64          `db:"Type"`
}

type WorkOrder struct {
	ID           string `db:"Id"`
	Lot          string `db:"Lot"`
	Manufactured int64  `db:"Manufactured"`
	Expiry       int64  `db:"Expiry"`
}

var lots map[string]WorkOrder

func exportLots(db *sqlx.DB) {
	rows, err := db.Queryx("select Id, Lot, Manufactured, Expiry from [dbo].[WorkOrder]")
	checkError("select * from [dbo].[WorkOrder] failed", err)

	file, err := os.Create("lots.csv.gz")
	checkError("Cannot create file", err)
	defer file.Close()

	gzw := gzip.NewWriter(file)
	defer gzw.Flush()
	defer gzw.Close()

	csvw := csv.NewWriter(gzw)
	defer csvw.Flush()

	csvw.Write([]string{"WorkOrderId:ID(Lot)", "Lot:string", "Manufactured:int", "Expiry:int"})

	lots = make(map[string]WorkOrder)

	for rows.Next() {
		wo := WorkOrder{}
		err = rows.StructScan(&wo)
		checkError("StructScan failed:", err)

		csvw.Write([]string{wo.ID, wo.Lot, strconv.FormatInt(wo.Manufactured, 10), strconv.FormatInt(wo.Expiry, 10)})

		lots[wo.ID] = wo
	}
}

func exportItems(db *sqlx.DB) {
	rows, err := db.Queryx("select NtinId,Serial,Status,ParentNtinId,ParentSerial,WorkOrderID,Sequence,Type from [dbo].[Item]")
	checkError("select * from [dbo].[WorkOrder] failed", err)

	itemsFile, err := os.Create("items.csv.gz")
	checkError("Cannot create file", err)
	defer itemsFile.Close()

	itemsGzip := gzip.NewWriter(itemsFile)
	defer itemsGzip.Flush()
	defer itemsGzip.Close()

	itemsWriter := csv.NewWriter(itemsGzip)
	defer itemsWriter.Flush()

	itemRelationFile, err := os.Create("itemrelations.csv.gz")
	checkError("Cannot create file", err)
	defer itemRelationFile.Close()

	itemRelationGzip := gzip.NewWriter(itemRelationFile)
	defer itemRelationGzip.Flush()
	defer itemRelationGzip.Close()

	itemRelationWriter := csv.NewWriter(itemRelationGzip)
	defer itemRelationWriter.Flush()

	lotRelationFile, err := os.Create("lotrelations.csv.gz")
	checkError("Cannot create file", err)
	defer lotRelationFile.Close()

	lotRelationGzip := gzip.NewWriter(lotRelationFile)
	defer lotRelationGzip.Flush()
	defer lotRelationGzip.Close()

	lotRelationWriter := csv.NewWriter(lotRelationGzip)
	defer lotRelationWriter.Flush()

	itemsWriter.Write([]string{"DbKey:ID(Item)", "Type:int", "Status:int", "Sequence:long", "Flags:string", "HelperCode:string"})
	itemRelationWriter.Write([]string{":START_ID(Item)", ":END_ID(Item)"})
	lotRelationWriter.Write([]string{":START_ID(Item)", ":END_ID(Lot)"})

	var i uint64

	for rows.Next() {
		item := Item{}
		err = rows.StructScan(&item)
		checkError("StructScan failed", err)

		ntin := ntins[item.NtinID]

		itemsWriter.Write([]string{
			ntin + item.Serial,
			strconv.FormatInt(item.Type, 10),
			strconv.FormatInt(item.Status, 10),
			strconv.FormatInt(item.Sequence, 10),
			"",
			""})

		if item.ParentNtinID.Valid {
			itemRelationWriter.Write([]string{
				ntins[item.ParentNtinID.Int64] + item.ParentSerial.String,
				ntin + item.Serial})
		}

		wo := lots[item.WorkOrderID]
		lotRelationWriter.Write([]string{ntin + item.Serial, wo.ID})

		i++
		if i%100000 == 0 {
			itemsWriter.Flush()
			itemRelationWriter.Flush()
			log.Println(i)
		}
	}
}

func exportNtins(db *sqlx.DB) {
	rows, err := db.Queryx("select * from dbo.NtinDefinition")
	checkError("select * from dbo.NtinDefinition failed", err)

	ntinsFile, err := os.Create("ntins.csv.gz")
	checkError("Cannot create file", err)
	defer ntinsFile.Close()

	ntinsGzip := gzip.NewWriter(ntinsFile)
	defer ntinsGzip.Flush()
	defer ntinsGzip.Close()

	ntinsWriter := csv.NewWriter(ntinsGzip)
	defer ntinsWriter.Flush()

	ntinsWriter.Write([]string{"DbKey:ID(NTIN)", "Id:int", "Ntin:string", "CodingRuleId:string"})

	for rows.Next() {
		dbkey := getEncodedTimeStamp()

		values := make(map[string]interface{})
		err = rows.MapScan(values)
		checkError("MapScan failed:", err)

		ntins[values["Id"].(int64)] = dbkey

		ntinsWriter.Write([]string{dbkey, strconv.FormatInt(values["Id"].(int64), 10), values["Ntin"].(string), values["CodingRuleId"].(string)})
	}
}

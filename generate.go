package main

import (
	"compress/gzip"
	"encoding/base32"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

var totalPallets *uint64

const PALLET_NTIN = ""

var palletIndex *uint64
var palletNumber *uint64

const CASE_NTIN = ""

var caseIndex uint64 = 0
var caseNumber uint64 = 4

const BUNDLE_NTIN = ""

var bundleIndex uint64 = 0
var bundleNumber uint64 = 8

const PACKAGE_NTIN = ""

var packageIndex uint64 = 0
var packageNumber uint64 = 100

var ntins = make(map[int64]string)

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

func main() {
	palletNumber = flag.Uint64("numberOfPallets", 1, "number of pallets to produce")
	palletIndex = flag.Uint64("palletIndex", 0, "starting pallet index")
	totalPallets = flag.Uint64("totalPallets", 10, "total number of pallets")
	flag.Parse()

	log.Println("Aggregating ", (*palletNumber), " Pallets starting from ", *palletIndex)

	rand.Seed(time.Now().UnixNano())

	log.Println("Exporting NTINs")
	exportNtins()
	log.Println("done")

	log.Println("Exporting LOTs")
	exportLots()
	log.Println("done")

	createLot("BENCHMARK", (*palletNumber))
	createLot("W_BRA_AGG_ALLBOUND_01_001", (*totalPallets)-(*palletNumber))
}

func createLot(lot string, palletsForThisLot uint64) {

	itemsFile, err := os.Create("items" + lot + ".csv.gz")
	checkError("Cannot create file", err)
	defer itemsFile.Close()
	itemRelationFile, err := os.Create("itemrelations" + lot + ".csv.gz")
	checkError("Cannot create file", err)
	defer itemRelationFile.Close()
	lotRelationFile, err := os.Create("lotrelations" + lot + ".csv.gz")
	checkError("Cannot create file", err)
	defer lotRelationFile.Close()

	itemsGzip := gzip.NewWriter(itemsFile)
	defer itemsGzip.Close()
	defer itemsGzip.Flush()
	itemRelationGzip := gzip.NewWriter(itemRelationFile)
	defer itemRelationGzip.Close()
	defer itemRelationGzip.Flush()
	lotRelationGzip := gzip.NewWriter(lotRelationFile)
	defer lotRelationGzip.Close()
	defer lotRelationGzip.Flush()

	itemsWriter := csv.NewWriter(itemsGzip)
	defer itemsWriter.Flush()
	itemRelationWriter := csv.NewWriter(itemRelationGzip)
	defer itemRelationWriter.Flush()
	lotRelationWriter := csv.NewWriter(lotRelationGzip)
	defer lotRelationWriter.Flush()

	itemsWriter.Write([]string{"DbKey:ID(Item)", "Type:int", "Status:int", "Sequence:long", "Flags:string", "HelperCode:string"})
	itemRelationWriter.Write([]string{":START_ID(Item)", ":END_ID(Item)"})
	lotRelationWriter.Write([]string{":START_ID(Item)", ":END_ID(Lot)"})

	casesForThisLot := palletsForThisLot * caseNumber
	bundlesForThisLot := casesForThisLot * bundleNumber
	packagesForThisLot := bundlesForThisLot * packageNumber
	log.Printf("Starting lot %s with %d pallets (estimated %d total items)", lot, palletsForThisLot, palletsForThisLot+casesForThisLot+bundlesForThisLot+packagesForThisLot)

	var i uint64 = 0
	for ; i < palletsForThisLot; i++ {
		createPallet(PALLET_NTIN, itemsWriter, itemRelationWriter, lot)
	}

	log.Println("lot terminated")
}

func exportLots() {
	file, err := os.Create("lots.csv.gz")
	checkError("Cannot create file", err)
	defer file.Close()

	gzw := gzip.NewWriter(file)
	defer gzw.Close()
	defer gzw.Flush()

	csvw := csv.NewWriter(gzw)
	defer csvw.Flush()

	csvw.Write([]string{"WorkOrderId:ID(Lot)", "Lot:string", "Manufactured:int", "Expiry:int"})
	csvw.Write([]string{"BENCHMARK", "BENCHMARK", "20161021", "20191231"})
	csvw.Write([]string{"W_BRA_AGG_ALLBOUND_01_001", "W_BRA_AGG_ALLBOUND_01_001", "20161021", "20191231"})
}

func exportNtins() {
	ntinsFile, err := os.Create("ntins.csv.gz")
	checkError("Cannot create file", err)
	defer ntinsFile.Close()

	ntinsGzip := gzip.NewWriter(ntinsFile)
	defer ntinsGzip.Close()
	defer ntinsGzip.Flush()

	ntinsWriter := csv.NewWriter(ntinsGzip)
	defer ntinsWriter.Flush()

	ntinsWriter.Write([]string{"DbKey:ID(NTIN)", "Id:int", "Ntin:string", "CodingRuleId:string"})

	dbkey := getEncodedTimeStamp()
	ntins[1023] = dbkey
	ntinsWriter.Write([]string{dbkey, "1023", "08691234", "GS1_SSCC"})

	dbkey = getEncodedTimeStamp()
	ntins[1024] = dbkey
	ntinsWriter.Write([]string{dbkey, "1024", "08691234", "CHINESE_EDMC"})

	dbkey = getEncodedTimeStamp()
	ntins[1024] = dbkey
	ntinsWriter.Write([]string{dbkey, "1024", "08691234", "CHINESE_EDMC"})

	dbkey = getEncodedTimeStamp()
	ntins[1024] = dbkey
	ntinsWriter.Write([]string{dbkey, "1024", "08691234", "CHINESE_EDMC"})

}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}

func createPallet(ntin string, itemsWriter *csv.Writer, itemRelationWriter *csv.Writer, lot string) {

	t := time.Now()

	fullKey := fmt.Sprintf("%s%010d", ntin, *palletIndex)

	err := itemsWriter.Write([]string{fullKey, "400", fmt.Sprintf("%010d", *palletIndex), "1", lot, strconv.FormatUint(*palletIndex, 10), "", ""})
	checkError("Cannot create PALLET", err)

	var j uint64 = 0
	for ; j < caseNumber; j++ {
		createCase(fullKey, CASE_NTIN, itemsWriter, itemRelationWriter, lot)
	}

	log.Printf("PALLET %010d done in %v", *palletIndex, time.Since(t))
	itemsWriter.Flush()
	itemRelationWriter.Flush()

	(*palletIndex)++
}

func createCase(parentFullKey string, ntin string, itemsWriter *csv.Writer, itemRelationWriter *csv.Writer, lot string) {

	fullKey := fmt.Sprintf("%s%010d", ntin, caseIndex)

	err := itemsWriter.Write([]string{fullKey, "300", fmt.Sprintf("%010d", caseIndex), "10", lot, strconv.FormatUint(caseIndex, 10), "", ""})
	checkError("Cannot create CASE", err)

	var k uint64 = 0
	for ; k < bundleNumber; k++ {
		createBundle(fullKey, BUNDLE_NTIN, itemsWriter, itemRelationWriter, lot)
	}

	err = itemRelationWriter.Write([]string{parentFullKey, fullKey})
	checkError("Cannot create CASE relation", err)

	caseIndex++
}

func createBundle(parentFullKey string, ntin string, itemsWriter *csv.Writer, itemRelationWriter *csv.Writer, lot string) {

	fullKey := fmt.Sprintf("%s%010d", ntin, bundleIndex)

	err := itemsWriter.Write([]string{fullKey, "200", fmt.Sprintf("%010d", bundleIndex), "10", lot, strconv.FormatUint(bundleIndex, 10), "", ""})
	checkError("Cannot create BUNDLE", err)

	var l uint64 = 0
	for ; l < packageNumber; l++ {
		createPackage(fullKey, PACKAGE_NTIN, itemsWriter, itemRelationWriter, lot)
	}

	err = itemRelationWriter.Write([]string{parentFullKey, fullKey})
	checkError("Cannot create BUNDLE relation", err)

	bundleIndex++
}

func createPackage(parentFullKey string, ntin string, itemsWriter *csv.Writer, itemRelationWriter *csv.Writer, lot string) {

	fullKey := fmt.Sprintf("%s%010d", ntin, packageIndex)

	err := itemsWriter.Write([]string{fullKey, "100", fmt.Sprintf("%010d", packageIndex), "10", lot, strconv.FormatUint(packageIndex, 10), "", ""})
	checkError("Cannot create PACKAGE", err)

	err = itemRelationWriter.Write([]string{parentFullKey, fullKey})
	checkError("Cannot create PACKAGE relation", err)

	packageIndex++
}

package main

import (
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"math/rand"
	"time"
	"math"
)

var palletIndex int = 0
var palletNumber *int
var caseIndex = 0
var caseNumber = 72

var bundleIndex = 0
var bundleNumber = 12

var packageIndex = 0
var packageNumber = 36

const MIN_ITEMS_PER_LOT = 300000
const MAX_ITEMS_PER_LOT = 700000

func main() {
	palletNumber = flag.Int("numberOfPallets", 1, "number of pallets to produce")
	flag.Parse()

	log.Println("Aggregating ", *palletNumber, " Pallets starting from ", palletIndex)

	ntinsFile, err := os.Create("ntins.csv.gz")
	checkError("Cannot create file", err)
	defer ntinsFile.Close()

	ntinsGzip := gzip.NewWriter(ntinsFile)
	defer ntinsGzip.Flush()
	defer ntinsGzip.Close()

	ntinsWriter := csv.NewWriter(ntinsGzip)
	defer ntinsWriter.Flush()

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

	ntinRelationFile, err := os.Create("ntinrelations.csv.gz")
	checkError("Cannot create file", err)
	defer ntinRelationFile.Close()

	ntinRelationGzip := gzip.NewWriter(ntinRelationFile)
	defer ntinRelationGzip.Flush()
	defer ntinRelationGzip.Close()

	ntinRelationWriter := csv.NewWriter(ntinRelationGzip)
	defer ntinRelationWriter.Flush()

	ntinsWriter.Write([]string{"NTIN:string:ID(NTIN)", "CodingSet:string"})
	itemsWriter.Write([]string{":ID(Item)", "Type:int", "Serial:string", "Status:int", "Lot:string", "Sequence:long", "Flags:string", "HelperCode:string"})
	itemRelationWriter.Write([]string{":START_ID(Item)", ":END_ID(Item)"})
	ntinRelationWriter.Write([]string{":START_ID(NTIN)", ":END_ID(Item)"})

	totalItems := (*palletNumber) * caseNumber * bundleNumber * packageNumber

	rand.Seed(time.Now().UnixNano())

	//eccediamo
	numberOfLots := int(math.Ceil(math.Max(float64(totalItems / MIN_ITEMS_PER_LOT), 1)))
	lots := make([]string, numberOfLots)

	for i:=0; i< numberOfLots;i++ {
		lots = append(lots, fmt.Sprintf("LOT%05d", rand.Intn(9999)))
	}

	for _, lot := range lots {

		palletsForThisLot := MIN_ITEMS_PER_LOT + rand.Intn(MAX_ITEMS_PER_LOT - MIN_ITEMS_PER_LOT)

		for i:=0; i < palletsForThisLot && palletIndex < (*palletNumber); i++ {
			createPallet("08691234", itemsWriter, ntinRelationWriter, itemRelationWriter, lot)
		}

	}


}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}

func createPallet(ntin string, itemsWriter *csv.Writer, ntinRelationWriter *csv.Writer, itemRelationWriter *csv.Writer, lot string) {
	log.Print("Inserting PALLET...")
	err := itemsWriter.Write([]string{fmt.Sprintf("%d%010d", ntin, palletIndex), "400", fmt.Sprintf("%010d", palletIndex), "1", lot, strconv.Itoa(palletIndex), "", ""})
	checkError("Cannot create PALLET", err)
	err = ntinRelationWriter.Write([]string{ntin, fmt.Sprintf("%d%010d", ntin, palletIndex)})
	checkError("Cannot create NTIN->PALLET", err)

	for j := 0; j < caseNumber; j++ {
		createCase(ntin, "08695678", itemsWriter, ntinRelationWriter, itemRelationWriter, lot)
	}

	palletIndex++

	log.Print("...PALLET done")
	itemsWriter.Flush()
	itemRelationWriter.Flush()
	ntinRelationWriter.Flush()
}

func createCase(parentFullKey string, ntin string, itemsWriter *csv.Writer, ntinRelationWriter *csv.Writer, itemRelationWriter *csv.Writer, lot string) {

	fullKey := fmt.Sprintf("%d%010d", ntin, caseIndex)

	err := itemsWriter.Write([]string{fullKey, "300", fmt.Sprintf("%010d", caseIndex), "10", lot, strconv.Itoa(caseIndex), "", ""})
	checkError("Cannot create CASE", err)
	err = ntinRelationWriter.Write([]string{ntin, fullKey})
	checkError("Cannot create NTIN->CASE", err)

	for k := 0; k < bundleNumber; k++ {
		createBundle(fullKey, "08699012", itemsWriter, ntinRelationWriter, itemRelationWriter, lot)
	}

	err = itemRelationWriter.Write([]string{parentFullKey, fullKey})
	checkError("Cannot create CASE relation", err)

	caseIndex++
}

func createBundle(parentFullKey string, ntin string, itemsWriter *csv.Writer, ntinRelationWriter *csv.Writer, itemRelationWriter *csv.Writer, lot string) {

	fullKey := fmt.Sprintf("%d%010d", ntin, bundleIndex)

	err := itemsWriter.Write([]string{fullKey, "200", fmt.Sprintf("%010d", bundleIndex), "10", lot, strconv.Itoa(bundleIndex), "", ""})
	checkError("Cannot create BUNDLE", err)
	err = ntinRelationWriter.Write([]string{ntin, fullKey})
	checkError("Cannot create NTIN->BUNDLE", err)

	for l := 0; l < packageNumber; l++ {
		createPackage(fullKey, "08690000", itemsWriter, ntinRelationWriter, itemRelationWriter, lot)
	}

	err = itemRelationWriter.Write([]string{parentFullKey, fullKey})
	checkError("Cannot create BUNDLE relation", err)

	bundleIndex++
}

func createPackage(parentFullKey string, ntin string, itemsWriter *csv.Writer, ntinRelationWriter *csv.Writer, itemRelationWriter *csv.Writer, lot string) {

	fullKey := fmt.Sprintf("%d%010d", ntin, packageIndex)

	err := itemsWriter.Write([]string{fullKey, "100", fmt.Sprintf("%010d", packageIndex), "10", lot, strconv.Itoa(packageIndex), "", ""})
	checkError("Cannot create PACKAGE", err)

	err = ntinRelationWriter.Write([]string{ntin, fullKey})
	checkError("Cannot create NTIN->BUNDLE", err)

	err = itemRelationWriter.Write([]string{parentFullKey, fullKey})
	checkError("Cannot create PACKAGE relation", err)

	packageIndex++
}
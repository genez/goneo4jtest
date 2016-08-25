package main

import (
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
)

func main() {
	palletIndex := 0
	palletNumber := flag.Int("numberOfPallets", 1, "number of pallets to produce")
	flag.Parse()

	log.Println("Aggregating ", *palletNumber, " Pallets starting from ", palletIndex)

	caseIndex := 0
	caseNumber := 8

	bundleIndex := 0
	bundleNumber := 12

	packageIndex := 0
	packageNumber := 24

	itemsFile, err := os.Create("items.csv.gz")
	checkError("Cannot create file", err)
	defer itemsFile.Close()

	itemsGzip := gzip.NewWriter(itemsFile)
	defer itemsGzip.Flush()
	defer itemsGzip.Close()

	itemsWriter := csv.NewWriter(itemsGzip)
	defer itemsWriter.Flush()

	relationsFile, err := os.Create("relations.csv.gz")
	checkError("Cannot create file", err)
	defer relationsFile.Close()

	relationsGzip := gzip.NewWriter(relationsFile)
	defer relationsGzip.Flush()
	defer relationsGzip.Close()

	relationsWriter := csv.NewWriter(relationsGzip)
	defer relationsWriter.Flush()

	itemsWriter.Write([]string{":ID(Item)", "Type:int", "NTIN:string", "Serial:string", "Status:int", "Lot:string", "Sequence:long", "Flags:string", "HelperCode:string"})
	relationsWriter.Write([]string{":START_ID(Item)",":END_ID(Item)"})

	for i := 0; i < (*palletNumber); i++ {
		log.Print("Inserting PALLET...")
		err := itemsWriter.Write([]string{fmt.Sprintf("08691234%010d", palletIndex), "400", "08691234", fmt.Sprintf("%010d", palletIndex), "1", "LOT001", strconv.Itoa(palletIndex), "", ""})
		checkError("Cannot create PALLET", err)

		for j := 0; j < caseNumber; j++ {
			err := itemsWriter.Write([]string{fmt.Sprintf("08695678%010d", caseIndex), "300", "08695678", fmt.Sprintf("%010d", caseIndex), "10", "LOT001", strconv.Itoa(caseIndex), "", ""})
			checkError("Cannot create CASE", err)

			for k := 0; k < bundleNumber; k++ {
				err := itemsWriter.Write([]string{fmt.Sprintf("08699012%010d", bundleIndex), "200", "08699012", fmt.Sprintf("%010d", bundleIndex), "10", "LOT001", strconv.Itoa(bundleIndex), "", ""})
				checkError("Cannot create BUNDLE", err)

				for l := 0; l < packageNumber; l++ {
					err := itemsWriter.Write([]string{fmt.Sprintf("08690000%010d", packageIndex), "100", "08690000", fmt.Sprintf("%010d", packageIndex), "10", "LOT001", strconv.Itoa(packageIndex), "", ""})
					checkError("Cannot create PACKAGE", err)

					err = relationsWriter.Write([]string{fmt.Sprintf("08699012%010d", bundleIndex), fmt.Sprintf("08690000%010d", packageIndex)})
					checkError("Cannot create PACKAGE relation", err)

					packageIndex++
				}

				err = relationsWriter.Write([]string{fmt.Sprintf("08695678%010d", caseIndex), fmt.Sprintf("08699012%010d", bundleIndex)})
				checkError("Cannot create BUNDLE relation", err)

				bundleIndex++
			}

			err = relationsWriter.Write([]string{fmt.Sprintf("08691234%010d", palletIndex), fmt.Sprintf("08695678%010d", caseIndex)})
			checkError("Cannot create PALLET relation", err)

			caseIndex++
		}

		palletIndex++

		log.Print("...PALLET done")
		itemsWriter.Flush()
	}
}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}

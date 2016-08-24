package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
)

func main() {
	palletIndex := flag.Int("firstPallet", 1, "first pallet to start from")
	palletNumber := flag.Int("numberOfPallets", 1, "number of pallets to produce")
	fileName := flag.String("fileName", "items.csv", "file name")
	flag.Parse()

	log.Println("Aggregating ", *palletNumber, " Pallets starting from ", *palletIndex)

	caseIndex := 1
	caseNumber := 36

	bundleIndex := 0
	bundleNumber := 8

	packageIndex := 0
	packageNumber := 96

	file, err := os.Create(*fileName)
	checkError("Cannot create file", err)
	defer file.Close()

	writer := csv.NewWriter(file)

	writer.Write([]string{"Type", "NTIN", "Serial", "ParentNTIN", "ParentSerial", "Status", "Lot", "Sequence", "Flags", "HelperCode"})

	defer writer.Flush()

	for i := 0; i < (*palletNumber); i++ {

		log.Print("Inserting PALLET...")
		err := writer.Write([]string{"400", "08691234", fmt.Sprintf("%010d", *palletIndex), "", "", "1", "LOT001", strconv.Itoa(*palletIndex), "", ""})
		checkError("Cannot create PALLET", err)

		for j := 0; j < caseNumber; j++ {
			err := writer.Write([]string{"300", "08695678", fmt.Sprintf("%03d%07d", *palletIndex, caseIndex), "08691234", fmt.Sprintf("%010d", *palletIndex), "10", "LOT001", strconv.Itoa(caseIndex), "", ""})
			checkError("Cannot create CASE", err)

			for k := 0; k < bundleNumber; k++ {
				err := writer.Write([]string{"200", "08699012", fmt.Sprintf("%03d%07d", *palletIndex, bundleIndex), "08695678", fmt.Sprintf("%03d%07d", *palletIndex, caseIndex), "10", "LOT001", strconv.Itoa(bundleIndex), "", ""})
				checkError("Cannot create BUNDLE", err)

				for l := 0; l < packageNumber; l++ {
					err := writer.Write([]string{"100", "08690000", fmt.Sprintf("%03d%07d", *palletIndex, packageIndex), "08699012", fmt.Sprintf("%03d%07d", *palletIndex, bundleIndex), "10", "LOT001", strconv.Itoa(packageIndex), "", ""})
					checkError("Cannot create PACKAGE", err)

					packageIndex++
				}

				bundleIndex++
			}

			caseIndex++
		}

		(*palletIndex)++

		log.Print("...PALLET done")
		writer.Flush()
	}
}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}

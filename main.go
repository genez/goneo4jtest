package main

import (
	"database/sql"
	"log"

	"fmt"
	_ "gopkg.in/cq.v1"
	"flag"
)

func main() {
	palletIndex := flag.Int("firstPallet", 1, "first pallet to start from")
	palletNumber := flag.Int("numberOfPallets", 300, "number of pallets to produce")
	flag.Parse()

	log.Println("Aggregating ", *palletNumber, " Pallets starting from ", *palletIndex)

	log.Print("Connecting...")
	db, err := sql.Open("neo4j-cypher", "http://neo4j:antares1@localhost:7474")
	if err != nil {
		log.Fatal(err)
	}
	log.Print("...done")
	defer db.Close()

	log.Print("Ping...")
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	log.Print("...done")



	caseIndex := 1
	caseNumber := 36

	bundleIndex := 0
	bundleNumber := 8

	packageIndex := 0
	packageNumber := 96

	for i := 0; i < (*palletNumber); i++ {
		/*log.Print("Begin tran...")
		tx, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}
		log.Print("...done")
		*/

		stmt, err := db.Prepare("CREATE (i:Item {Type:{0}, NTIN:{1}, Serial:{2}, Status:{3}, Lot:{4}, Sequence:{5}, Flags:{6}, HelperCode:{7}}) RETURN id(i) as node_id;")
		if err != nil {
			log.Fatal(err)
		}
		defer stmt.Close()

		rel, err := db.Prepare("MATCH(p:Item),(c:Item) WHERE id(p)={0} AND id(c)={1} CREATE (p)-[:CONTAINS]->(c)")
		if err != nil {
			log.Fatal(err)
		}
		defer rel.Close()

		log.Print("Inserting PALLET...")
		var palletId int
		err = stmt.QueryRow(400, "08691234", fmt.Sprintf("%010d", palletIndex), 1, "LOT001", palletIndex, "", "").Scan(&palletId)
		if err != nil {
			log.Fatal(err)
		}

		for j := 0; j < caseNumber; j++ {
			log.Print("Inserting CASE...")
			var caseId int
			err = stmt.QueryRow(300, "08695678", fmt.Sprintf("%03d%07d", palletIndex, caseIndex), 10, "LOT001", caseIndex, "", "").Scan(&caseId)
			_, err = rel.Exec(palletId, caseId)
			if err != nil {
				log.Fatal(err)
			}

			for k := 0; k < bundleNumber; k++ {
				log.Print("Inserting BUNDLE...")
				var bundleId int
				err = stmt.QueryRow(200, "08699012", fmt.Sprintf("%03d%07d", palletIndex, bundleIndex), 10, "LOT001", bundleIndex, "", "").Scan(&bundleId)
				_, err = rel.Exec(caseId, bundleId)
				if err != nil {
					log.Fatal(err)
				}

				for l := 0; l < packageNumber; l++ {
					var packageId int
					err = stmt.QueryRow(100, "08690000", fmt.Sprintf("%03d%07d", palletIndex, packageIndex), 10, "LOT001", packageIndex, "", "").Scan(&packageId)
					_, err = rel.Exec(bundleId, packageId)
					if err != nil {
						log.Fatal(err)
					}

					packageIndex++
				}

				bundleIndex++

				log.Print("...BUNDLE done")
			}

			caseIndex++

			log.Print("...CASE done")
		}

		(*palletIndex)++

		/*err = tx.Commit()
		if err != nil {
			log.Fatal(err)
		}*/
		log.Print("...PALLET done")
	}

}

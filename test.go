package main

import (
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"fmt"
)

func main() {
	driver := bolt.NewDriver()
	conn, _ := driver.OpenNeo("bolt://neo4j:antares1@139.59.143.205:7687")
	defer conn.Close()

	for {
		// Lets get the node
    	data, err := conn.QueryNeo("MATCH (i:Item) return count(i);", nil)

	fmt.Printf("%v", data)
	fmt.Printf("%v", err)

		data.All()
		data.Close()
	}



}


#!/usr/bin/env bash

#this will create two files: items.csv.gz and relations.csv.gz
#16G     items.csv.gz
#5.5G    relations.csv.gz
#the tree is composed of 4 levels
#868000x8x12x24 = more less 2 bln nodes
./goneo4jtest --numberOfPallets=868000

#paths should be adjusted
neo4j-import --into /mnt/volume-nyc1-01/neo4j-data/databases/graph.db --id-type string --nodes:Item items.csv.gz --relationships:CONTAINS relations.csv.gz

neo4j-shell -c "CREATE INDEX ON :Item(NTIN);"
neo4j-shell -c "CREATE INDEX ON :Item(Serial);"
neo4j-shell -c "schema await;"


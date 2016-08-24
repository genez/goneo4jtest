USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///f.csv" AS line
CREATE (:Item { Type: toInt(line.Type), NTIN:line.NTIN, Serial:line.Serial, ParentNTIN:line.ParentNTIN, ParentSerial:line.ParentSerial, Status: toInt(line.Status), Lot:line.Lot, Sequence: toInt(line.Sequence), Flags:line.Flags, HelperCode:line.HelperCode});
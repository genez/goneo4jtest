MATCH (c:Item),(p:Item)
WHERE c.ParentNTIN=p.NTIN AND c.ParentSerial=p.Serial
CREATE (p)-[:CONTAINS]->(c);
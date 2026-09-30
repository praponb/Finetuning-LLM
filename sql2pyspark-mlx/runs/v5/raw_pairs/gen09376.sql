SELECT * FROM (SELECT region, city, sqft FROM stores) src PIVOT (MAX(sqft) AS amount, COUNT(1) AS n FOR city IN ('Tokyo' AS tokyo, 'Bangkok' AS bangkok))

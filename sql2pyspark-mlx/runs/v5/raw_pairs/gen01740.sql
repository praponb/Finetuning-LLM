SELECT * FROM (SELECT region, city, sqft FROM stores WHERE sqft = 12795)
PIVOT (SUM(sqft) FOR city IN ('Chiang Mai', 'London', 'Singapore'))
ORDER BY region;

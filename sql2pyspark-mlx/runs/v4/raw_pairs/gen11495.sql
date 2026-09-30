SELECT * FROM (SELECT region, city, sqft FROM stores) PIVOT (SUM(sqft) FOR city IN ('Chiang Mai', 'Berlin'));

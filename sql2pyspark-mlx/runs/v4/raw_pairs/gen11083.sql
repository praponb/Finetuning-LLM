SELECT * FROM (SELECT country, city, rating FROM suppliers) src PIVOT (MAX(rating) AS value_sum, COUNT(1) AS cnt FOR city IN ('Tokyo' AS tokyo, 'Chiang Mai' AS chiang_mai))

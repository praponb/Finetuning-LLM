SELECT * FROM (SELECT segment, country, city, age FROM customers) src PIVOT (AVG(age) FOR city IN ('Berlin' AS berlin, 'Tokyo' AS tokyo, 'Chiang Mai' AS chiang_mai))

SELECT * FROM (SELECT country, city, age FROM customers) src PIVOT (MAX(age) AS amount, COUNT(1) AS n FOR city IN ('Berlin' AS berlin, 'Bangkok' AS bangkok, 'Chiang Mai' AS chiang_mai))

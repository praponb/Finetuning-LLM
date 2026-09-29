SELECT * FROM (SELECT country, city, age FROM customers) src PIVOT (MAX(age) FOR city IN ('Bangkok' AS bangkok, 'Berlin' AS berlin));

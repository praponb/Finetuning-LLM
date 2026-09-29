SELECT * FROM (SELECT country, city, age FROM customers)
PIVOT (SUM(age) FOR city IN ('Tokyo', 'London', 'Berlin', 'Singapore'));

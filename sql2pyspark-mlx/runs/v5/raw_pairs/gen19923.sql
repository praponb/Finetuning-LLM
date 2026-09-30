SELECT * FROM (SELECT country, city, credit_limit FROM customers)
PIVOT (COUNT(credit_limit) FOR city IN ('Berlin', 'Singapore', 'London', 'Chiang Mai'))
ORDER BY country

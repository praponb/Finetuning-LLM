SELECT * FROM (SELECT segment, city, credit_limit FROM customers)
PIVOT (MAX(credit_limit) FOR city IN ('Tokyo', 'Berlin', 'Chiang Mai', 'London'))
ORDER BY segment;

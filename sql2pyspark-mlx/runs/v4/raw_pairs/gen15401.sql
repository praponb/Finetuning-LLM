SELECT * FROM (SELECT is_active, city, credit_limit FROM customers)
PIVOT (AVG(credit_limit) FOR city IN ('New York', 'Bangkok', 'London'))
ORDER BY is_active;

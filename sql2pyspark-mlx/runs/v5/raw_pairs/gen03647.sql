SELECT country FROM customers WHERE age <= 61 UNION SELECT country FROM suppliers WHERE city IN ('New York', 'Chiang Mai', 'London') ORDER BY country

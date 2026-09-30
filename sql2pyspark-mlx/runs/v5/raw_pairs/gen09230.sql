SELECT * FROM (SELECT country, city, rating FROM suppliers WHERE supplier_name LIKE '%GmbH%')
PIVOT (MAX(rating) FOR city IN ('Tokyo', 'Singapore', 'Berlin', 'New York'));

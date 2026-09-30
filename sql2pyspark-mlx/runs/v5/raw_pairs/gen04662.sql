SELECT * FROM (SELECT country, city, rating FROM suppliers)
PIVOT (AVG(rating) FOR city IN ('Singapore', 'New York', 'London'))
ORDER BY country

SELECT * FROM (SELECT country, city, rating FROM suppliers) PIVOT (MAX(rating) FOR city IN ('Singapore', 'Chiang Mai', 'Berlin')) ORDER BY country;

SELECT * FROM (SELECT country, city, rating FROM suppliers) PIVOT (COUNT(rating) FOR city IN ('Bangkok', 'Chiang Mai'));

SELECT * FROM (SELECT country, city, rating FROM suppliers) src PIVOT (AVG(rating) AS value_sum, COUNT(1) AS cnt FOR city IN ('London', 'Bangkok'))

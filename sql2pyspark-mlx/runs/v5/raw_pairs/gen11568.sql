SELECT * FROM (SELECT city, country, rating FROM suppliers) src PIVOT (SUM(rating) AS total, COUNT(1) AS cnt FOR country IN ('SG', 'DE'))

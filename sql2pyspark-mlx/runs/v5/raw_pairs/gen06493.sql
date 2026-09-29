SELECT id, country, segment, age, ROUND(age / SUM(age) OVER (PARTITION BY country, segment), 3) AS age_share FROM customers

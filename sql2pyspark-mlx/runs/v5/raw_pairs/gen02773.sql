SELECT id, segment, age, ROUND(age / SUM(age) OVER (PARTITION BY segment), 3) AS age_share FROM customers WHERE age BETWEEN 31 AND 42

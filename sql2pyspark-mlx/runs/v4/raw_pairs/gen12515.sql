SELECT id, segment, age, ROUND(age / SUM(age) OVER (PARTITION BY segment), 3) AS age_share FROM customers WHERE name LIKE 'J%';

SELECT * FROM (SELECT is_active, ROUND(AVG(age), 2) AS avg_age FROM customers GROUP BY is_active) AS s WHERE avg_age > 21

SELECT segment, is_active, MAX(age) AS max_age FROM customers GROUP BY segment, is_active HAVING MAX(age) > 42 ORDER BY segment ASC, 3 DESC

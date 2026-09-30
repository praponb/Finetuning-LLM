SELECT city, SUM(CASE WHEN country = 'JP' THEN 1 ELSE 0 END) AS jp_count FROM customers GROUP BY city;

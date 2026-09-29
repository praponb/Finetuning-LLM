SELECT segment, unique_segments FROM (SELECT segment, COUNT(DISTINCT age) AS unique_segments FROM customers WHERE name IS NOT NULL GROUP BY segment) AS x WHERE unique_segments > 4

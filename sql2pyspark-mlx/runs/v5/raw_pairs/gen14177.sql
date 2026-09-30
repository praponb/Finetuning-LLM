SELECT category, COUNT(DISTINCT price) AS unique_categorys, MIN(cost) AS min_cost FROM products GROUP BY 1 ORDER BY 2 ASC, category ASC LIMIT 5

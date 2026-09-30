SELECT brand, total_cost
FROM (
    SELECT brand, SUM(cost) AS total_cost
    FROM products
    GROUP BY brand
) AS s
WHERE total_cost > 2300
ORDER BY brand
LIMIT 1

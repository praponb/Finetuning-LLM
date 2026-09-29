SELECT brand, min_cost
FROM (
    SELECT brand, MIN(cost) AS min_cost
    FROM products
    GROUP BY brand
) AS agg
WHERE min_cost > 190

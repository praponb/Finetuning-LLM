SELECT category, min_cost, fashion_count
FROM (
    SELECT category, MIN(cost) AS min_cost, COUNT(CASE WHEN category = 'Fashion' THEN 1 END) AS fashion_count
    FROM products
    WHERE LENGTH(product_name) < 7
    GROUP BY category
) AS s
WHERE min_cost >= 680
ORDER BY fashion_count DESC, category ASC
LIMIT 10;

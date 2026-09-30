SELECT
    CASE WHEN price >= 1030 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_tier,
    COUNT(*) AS num_rows,
    COUNT(CASE WHEN category = 'Grocery' THEN 1 END) AS grocery_count,
    ROUND(AVG(cost), 2) AS avg_cost
FROM products
GROUP BY CASE WHEN price >= 1030 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY num_rows DESC;

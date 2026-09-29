SELECT
    category,
    MAX(price) - MIN(price) price_range,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) category_values,
    ROUND((100 * SUM(CASE WHEN category = 'Grocery' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_grocery
FROM products
GROUP BY 1;

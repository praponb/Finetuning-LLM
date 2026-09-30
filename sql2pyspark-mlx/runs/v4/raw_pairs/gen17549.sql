SELECT
    DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
    DATE_TRUNC('MONTH', launch_date) AS activity_month,
    category,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    SIZE(COLLECT_SET(category)) AS num_categories,
    COUNT(*) AS row_count,
    SUM(cost) FILTER (WHERE category = 'Grocery') AS grocery_total_cost,
    ROUND(SUM(price) / NULLIF(COUNT(CASE WHEN brand = 'Acme' THEN 1 END), 0), 2) AS price_per_acme
FROM products
GROUP BY DATE_TRUNC('QUARTER', launch_date), DATE_TRUNC('MONTH', launch_date), category
ORDER BY price_per_acme ASC

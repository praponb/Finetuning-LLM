SELECT
    CASE WHEN cost >= 760 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    category,
    ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN brand = 'Initech' THEN 1 END), 0), 2) AS stock_qty_per_initech,
    PERCENTILE_APPROX(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list
FROM products
GROUP BY CASE WHEN cost >= 760 THEN 'LARGE' ELSE 'SMALL' END, category;

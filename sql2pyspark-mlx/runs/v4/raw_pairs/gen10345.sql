WITH with_dims AS (
    SELECT
        e.*,
        p.cost,
        p.category,
        p.product_name
    FROM inventory e
    LEFT JOIN products p
        ON e.product_id = p.id
)
SELECT
    DATE_TRUNC('YEAR', last_restock) AS year_start,
    CASE WHEN quantity >= 210 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    COUNT(DISTINCT category) AS distinct_categories,
    COUNT(DISTINCT category) AS distinct_categories_2,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    COUNT(CASE WHEN category = 'Home' THEN 1 END) AS home_count,
    GROUPING_ID() AS grouping_id
FROM with_dims
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', last_restock), CASE WHEN quantity >= 210 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('YEAR', last_restock)));

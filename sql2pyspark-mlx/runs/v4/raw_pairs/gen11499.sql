WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        ROW_NUMBER() OVER (ORDER BY stock_qty, id) row_seq,
        NTILE(3) OVER (PARTITION BY supplier_id ORDER BY cost, id) cost_bucket
    FROM products
)
-- Multi-level aggregation
SELECT
    CASE WHEN cost >= 750 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_tier,
    category,
    CASE WHEN stock_qty >= 364 THEN 'HIGH' ELSE 'LOW' END AS stock_qty_tier,
    APPROX_PERCENTILE(stock_qty, 0.5) AS stock_qty_p50,
    COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END) AS umbrella_count,
    COUNT_IF(category = 'Sports') AS sports_events
FROM enriched
GROUP BY ROLLUP (CASE WHEN cost >= 750 THEN 'PREMIUM' ELSE 'STANDARD' END, category, CASE WHEN stock_qty >= 364 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY cost_tier DESC NULLS LAST;

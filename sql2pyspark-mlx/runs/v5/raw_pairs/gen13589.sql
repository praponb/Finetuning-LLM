WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock DESC NULLS FIRST, id) AS latest_rank
    FROM inventory
    QUALIFY latest_rank = 1
)
-- Grouped metrics
SELECT
    CASE WHEN quantity >= 243 THEN 'HIGH' ELSE 'LOW' END AS quantity_tier,
    DATE_TRUNC('WEEK', last_restock) AS week_start,
    PERCENTILE_APPROX(quantity, 0.95) AS quantity_p95,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    APPROX_PERCENTILE(quantity, 0.9) AS quantity_p90
FROM ranked
GROUP BY CASE WHEN quantity >= 243 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('WEEK', last_restock)
ORDER BY week_start NULLS LAST;

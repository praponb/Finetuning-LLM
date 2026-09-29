WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN quantity >= 214 THEN 'High' WHEN quantity >= 106 THEN 'Medium' ELSE 'Low' END AS quantity_band
    FROM inventory
    WHERE last_restock >= DATE '2024-04-14'
)
-- Multi-level aggregation
SELECT
    store_id,
    product_id,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(quantity) AS avg_quantity,
    PERCENTILE_APPROX(quantity, 0.9) AS quantity_p90,
    MIN(quantity) AS min_quantity,
    COUNT(DISTINCT id) AS distinct_ids_2,
    STDDEV_SAMP(quantity) AS quantity_stddev
FROM base
GROUP BY store_id, product_id;

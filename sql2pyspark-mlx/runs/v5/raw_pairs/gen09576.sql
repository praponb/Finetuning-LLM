WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        product_id,
        quantity,
        unit_price
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS dedup_rank
        FROM order_items
    ) x
    WHERE dedup_rank = 1
)
-- Grouped metrics
SELECT
    product_id,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(unit_price, 0.95) AS unit_price_p95,
    PERCENTILE_APPROX(unit_price, ARRAY(0.25, 0.5, 0.75)) AS unit_price_quartiles,
    CORR(quantity, unit_price) AS corr_quantity_unit_price,
    COUNT(*) AS event_count
FROM ranked
GROUP BY product_id
ORDER BY product_id

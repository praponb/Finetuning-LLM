WITH base AS (
    SELECT *, ROUND(unit_price / quantity, 2) AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND order_id IN (13, 39)
)
SELECT
    product_id,
    PERCENTILE_APPROX(unit_price, ARRAY(0.25, 0.5, 0.75)) AS unit_price_quartiles,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids
FROM base
GROUP BY ROLLUP (product_id)
ORDER BY num_rows DESC NULLS FIRST

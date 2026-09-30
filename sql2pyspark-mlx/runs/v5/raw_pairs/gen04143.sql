WITH base AS (
    SELECT
        *,
        unit_price * quantity line_total,
        ROUND(unit_price, 2) unit_price_rounded
    FROM order_items
    WHERE unit_price IS NOT NULL
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY unit_price DESC, id) row_seq,
        LAST_VALUE(line_total, TRUE) OVER (PARTITION BY product_id ORDER BY unit_price_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_line_total,
        CORR(quantity, unit_price) OVER (PARTITION BY order_id) corr_quantity_unit_price
    FROM base
),
deduped AS (
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY last_known_line_total NULLS LAST, id) = 1
),
cleaned AS (
    SELECT *, corr_quantity_unit_price + row_seq corr_quantity_unit_price_plus_row_seq
    FROM deduped
    WHERE product_id IS NOT NULL
)
SELECT
    product_id,
    CASE WHEN line_total >= 74 THEN 'HIGH' ELSE 'LOW' END AS line_total_tier,
    ROUND(AVG(line_total), 2) AS avg_line_total,
    APPROX_PERCENTILE(line_total, 0.5) AS line_total_p50,
    PERCENTILE_APPROX(line_total, 0.95) AS line_total_p95,
    DENSE_RANK() OVER (PARTITION BY product_id ORDER BY APPROX_PERCENTILE(line_total, 0.5) DESC) AS line_total_p50_dense_rank
FROM cleaned
GROUP BY product_id, CASE WHEN line_total >= 74 THEN 'HIGH' ELSE 'LOW' END
HAVING line_total_p95 >= 0
ORDER BY line_total_p95 NULLS FIRST;

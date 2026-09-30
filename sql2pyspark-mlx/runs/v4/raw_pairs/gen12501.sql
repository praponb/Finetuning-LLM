WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(quantity, 2) OVER (PARTITION BY order_id ORDER BY quantity, id) AS next_quantity,
        LEAD(unit_price, 1) OVER (ORDER BY unit_price, id) AS next_unit_price,
        CORR(quantity, unit_price) OVER (PARTITION BY product_id) AS corr_quantity_unit_price
    FROM order_items
)
-- Aggregate with conditional metrics
SELECT
    order_id,
    BIT_AND(quantity) AS quantity_bit_and,
    APPROX_PERCENTILE(quantity, 0.95) AS quantity_p95,
    BIT_OR(quantity) AS quantity_bit_or,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    APPROX_PERCENTILE(quantity, 0.5) AS quantity_p50,
    PERCENTILE_APPROX(next_unit_price, ARRAY(0.25, 0.5, 0.75)) AS next_unit_price_quartiles
FROM with_windows
GROUP BY order_id
HAVING quantity_bit_and >= 1
ORDER BY distinct_order_ids DESC NULLS LAST;

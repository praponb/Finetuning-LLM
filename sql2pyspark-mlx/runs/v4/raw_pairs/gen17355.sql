WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(quantity) IGNORE NULLS OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
        LAG(unit_price, 2, 0) OVER (PARTITION BY product_id ORDER BY quantity, id) AS prev_unit_price,
        AVG(unit_price) OVER (PARTITION BY product_id) AS product_id_avg_unit_price,
        PERCENT_RANK() OVER (ORDER BY unit_price DESC NULLS FIRST) AS unit_price_percent_rank
    FROM order_items
)
SELECT
    id,
    order_id,
    product_id,
    quantity,
    unit_price,
    first_known_quantity,
    prev_unit_price,
    unit_price_percent_rank
FROM enriched;

WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        SUM(unit_price) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_unit_price,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) row_seq
    FROM order_items
)
SELECT
    id,
    order_id,
    product_id,
    unit_price,
    running_unit_price,
    row_seq
FROM enriched
ORDER BY unit_price

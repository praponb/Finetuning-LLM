WITH scored AS (
    SELECT
        *,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY order_id, product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price
    FROM order_items
)
SELECT
    id,
    order_id,
    unit_price,
    running_quantity,
    last_known_unit_price
FROM scored;

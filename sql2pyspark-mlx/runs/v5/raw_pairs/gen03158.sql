WITH top_order_items AS (
    SELECT
        id,
        order_id,
        unit_price,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS rn
    FROM order_items
)
SELECT id, order_id, unit_price
FROM top_order_items
WHERE rn <= 5
ORDER BY unit_price DESC

SELECT
    id,
    order_id,
    unit_price,
    quantity
FROM (
    SELECT
        id,
        order_id,
        unit_price,
        quantity,
        RANK() OVER (PARTITION BY order_id ORDER BY unit_price) AS row_num
    FROM order_items
    WHERE quantity <= 4
) AS t
WHERE row_num = 1
ORDER BY quantity DESC

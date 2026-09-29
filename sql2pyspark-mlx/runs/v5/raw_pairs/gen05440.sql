SELECT
    id,
    order_id,
    quantity,
    unit_price
FROM (
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS row_num
    FROM order_items
    WHERE unit_price BETWEEN 110 AND 310
) AS ranked
WHERE row_num <= 2
ORDER BY quantity DESC

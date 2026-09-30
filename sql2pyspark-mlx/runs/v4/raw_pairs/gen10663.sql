SELECT id, product_id, quantity
FROM (
    SELECT
        id,
        product_id,
        quantity,
        RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) AS row_num
    FROM order_items
    WHERE unit_price > 210
) AS r
WHERE row_num = 1
ORDER BY quantity;

SELECT
    id,
    product_id,
    unit_price,
    quantity,
    LAG(quantity, 1) OVER (PARTITION BY product_id ORDER BY unit_price, id) AS prev_quantity
FROM order_items

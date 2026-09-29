SELECT
    id,
    order_id,
    quantity,
    product_id,
    quantity - unit_price AS line_total
FROM order_items
WHERE unit_price < 170 OR product_id = 39;

SELECT
    id,
    order_id,
    product_id,
    quantity + unit_price AS line_total
FROM order_items
WHERE order_id IN (3, 22, 27, 38);

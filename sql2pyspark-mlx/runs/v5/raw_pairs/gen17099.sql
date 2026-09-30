SELECT
    order_id,
    quantity,
    product_id,
    quantity + unit_price line_total
FROM order_items
WHERE order_id IN (6, 22) AND unit_price >= 290 AND product_id IS NULL
ORDER BY order_id DESC, line_total DESC;

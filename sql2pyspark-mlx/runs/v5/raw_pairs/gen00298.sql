SELECT
    oi.id,
    oi.order_id,
    oi.unit_price,
    oi.quantity
FROM order_items oi
WHERE oi.product_id IN (SELECT id FROM products WHERE YEAR(launch_date) >= 2024)

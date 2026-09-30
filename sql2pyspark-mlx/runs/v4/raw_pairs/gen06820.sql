SELECT
    order_items.id,
    order_items.unit_price,
    orders.order_date,
    products.brand,
    order_items.unit_price * order_items.quantity AS line_total
FROM order_items
JOIN orders ON order_items.order_id = orders.id
JOIN products ON order_items.product_id = products.id
WHERE products.product_name IS NOT NULL AND LOWER(TRIM(products.product_name)) = 'running shoes'

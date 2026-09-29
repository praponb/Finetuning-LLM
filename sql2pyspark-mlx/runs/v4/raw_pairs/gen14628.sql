SELECT
    order_items.id,
    order_items.quantity,
    products.product_name,
    products.supplier_id,
    orders.order_date
FROM order_items
INNER JOIN products ON order_items.product_id = products.id
INNER JOIN orders ON orders.id = order_items.order_id
WHERE order_items.product_id IN (7, 26, 28, 34)

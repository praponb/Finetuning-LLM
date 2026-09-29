SELECT
    products.id,
    products.category,
    COUNT(order_items.id) AS order_item_count,
    AVG(order_items.unit_price) AS avg_unit_price,
    MIN(order_items.quantity) AS min_quantity
FROM products
INNER JOIN order_items ON products.id = order_items.product_id
WHERE order_items.product_id = 1
GROUP BY products.id, products.category

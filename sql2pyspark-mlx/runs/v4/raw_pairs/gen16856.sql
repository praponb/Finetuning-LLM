SELECT
    products.price,
    products.category,
    order_items.order_id,
    order_items.quantity
FROM products
LEFT JOIN order_items ON order_items.product_id = products.id;

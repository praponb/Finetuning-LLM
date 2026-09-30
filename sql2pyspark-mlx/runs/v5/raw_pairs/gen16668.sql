SELECT
    order_items.order_id,
    orders.customer_id,
    orders.amount,
    products.launch_date
FROM order_items
INNER JOIN orders ON orders.id = order_items.order_id
INNER JOIN products ON products.id = order_items.product_id
WHERE products.cost >= 600;

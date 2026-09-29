SELECT
    order_items.id,
    order_items.quantity,
    orders.customer_id,
    customers.credit_limit,
    customers.email
FROM order_items
INNER JOIN orders ON order_items.order_id = orders.id
LEFT JOIN customers ON orders.customer_id = customers.id
WHERE customers.age = 66

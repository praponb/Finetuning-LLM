SELECT
    orders.channel,
    orders.status,
    customers.name,
    customers.city,
    stores.city AS store_city
FROM orders
LEFT OUTER JOIN customers ON orders.customer_id = customers.id
INNER JOIN stores ON stores.id = orders.store_id
WHERE customers.email IS NOT NULL AND YEAR(orders.order_date) = 2023

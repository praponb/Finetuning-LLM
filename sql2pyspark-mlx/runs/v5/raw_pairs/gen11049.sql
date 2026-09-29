SELECT
    orders.status,
    stores.store_name,
    stores.opened_date,
    customers.segment
FROM orders
INNER JOIN stores ON orders.store_id = stores.id
FULL OUTER JOIN customers ON orders.customer_id = customers.id

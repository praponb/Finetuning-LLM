SELECT
    orders.amount,
    orders.discount,
    customers.age,
    customers.is_active,
    stores.store_name,
    stores.region
FROM orders
INNER JOIN customers ON orders.customer_id = customers.id
INNER JOIN stores ON orders.store_id = stores.id
ORDER BY stores.region DESC;

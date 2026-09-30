SELECT
    orders.id,
    orders.status,
    orders.amount,
    stores.opened_date,
    customers.segment,
    customers.signup_date
FROM orders
LEFT JOIN stores ON orders.store_id = stores.id
INNER JOIN customers ON orders.customer_id = customers.id
WHERE customers.segment IS NOT NULL
ORDER BY customers.signup_date, segment

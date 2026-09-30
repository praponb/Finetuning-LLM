SELECT
    shipments.id,
    shipments.status,
    shipments.cost,
    orders.discount,
    orders.amount,
    customers.name,
    customers.credit_limit - customers.age AS credit_limit_minus_age
FROM shipments
JOIN orders ON shipments.order_id = orders.id
LEFT JOIN customers ON orders.customer_id = customers.id
WHERE orders.channel IN ('mobile', 'partner', 'store')
ORDER BY shipments.cost, id
LIMIT 10

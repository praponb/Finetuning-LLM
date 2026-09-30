SELECT
    shipments.cost,
    shipments.delivered_date,
    orders.amount,
    orders.channel,
    customers.name,
    customers.age
FROM shipments
LEFT JOIN orders ON shipments.order_id = orders.id
JOIN customers ON customers.id = orders.customer_id;

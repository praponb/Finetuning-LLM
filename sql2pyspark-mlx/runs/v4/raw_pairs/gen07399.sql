SELECT
    orders.discount,
    orders.shipping_fee,
    customers.country,
    customers.credit_limit
FROM orders
LEFT OUTER JOIN customers ON orders.customer_id = customers.id
WHERE orders.shipping_fee > 15
ORDER BY credit_limit DESC

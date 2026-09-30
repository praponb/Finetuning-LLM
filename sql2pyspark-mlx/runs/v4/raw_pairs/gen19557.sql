SELECT
    customers.id,
    customers.email,
    COUNT(orders.id) AS order_count,
    SUM(orders.shipping_fee) AS total_shipping_fee
FROM customers
LEFT JOIN orders ON orders.customer_id = customers.id
GROUP BY customers.id, customers.email
ORDER BY 1 ASC

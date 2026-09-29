SELECT
    customers.id,
    COUNT(orders.id) AS order_count,
    COUNT(DISTINCT orders.shipping_fee) AS unique_channels,
    MIN(orders.order_date) AS first_order_date
FROM customers
INNER JOIN orders ON orders.customer_id = customers.id
WHERE customers.country = 'TH'
GROUP BY customers.id

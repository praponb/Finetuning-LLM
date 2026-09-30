WITH orders_by_customer AS (
    SELECT customer_id, MAX(shipping_fee) - MIN(shipping_fee) AS shipping_fee_range
    FROM orders
    GROUP BY customer_id
)
SELECT c.name, c.email, obc.shipping_fee_range
FROM customers c
LEFT OUTER JOIN orders_by_customer obc ON c.id = obc.customer_id
WHERE obc.shipping_fee_range >= 35
ORDER BY 2

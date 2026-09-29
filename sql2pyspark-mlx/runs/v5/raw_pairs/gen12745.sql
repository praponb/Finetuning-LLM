WITH customer_stats AS (
    SELECT customer_id, MAX(order_date) AS latest_order_date
    FROM orders
    WHERE customer_id IS NULL
    GROUP BY customer_id
)
SELECT customers.id, customers.signup_date, customer_stats.latest_order_date
FROM customers
LEFT JOIN customer_stats ON customers.id = customer_stats.customer_id
ORDER BY signup_date DESC

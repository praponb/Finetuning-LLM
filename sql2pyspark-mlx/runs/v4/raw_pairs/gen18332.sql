WITH customer_totals AS (
    SELECT customer_id, COUNT(*) AS cnt, MAX(order_date) AS last_order_date
    FROM orders
    GROUP BY customer_id
)
SELECT
    customers.id,
    customers.segment,
    customers.age,
    customer_totals.cnt,
    customer_totals.last_order_date
FROM customers
JOIN customer_totals ON customers.id = customer_totals.customer_id
WHERE customer_totals.cnt > 10
ORDER BY 5, id
LIMIT 5;

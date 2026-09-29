WITH customer_stats AS (
    SELECT customer_id, MAX(order_date) AS last_order_date, COUNT(DISTINCT shipping_fee) AS unique_statuss
    FROM orders
    WHERE amount <= 860
    GROUP BY customer_id
)
SELECT
    c.id,
    c.age,
    c.segment,
    cs.last_order_date,
    cs.unique_statuss
FROM customers c
LEFT JOIN customer_stats cs ON c.id = cs.customer_id
WHERE cs.unique_statuss >= 5
ORDER BY c.segment;

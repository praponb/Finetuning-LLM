WITH order_stats AS (
    SELECT order_id, COUNT(DISTINCT amount) AS unique_methods
    FROM payments
    GROUP BY order_id
)
SELECT o.channel, o.amount, os.unique_methods
FROM orders o
LEFT OUTER JOIN order_stats os ON o.id = os.order_id
ORDER BY os.unique_methods

WITH order_stats AS (
    SELECT order_id, COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS success_count
    FROM payments
    WHERE order_id = 36
    GROUP BY order_id
)
SELECT
    o.id,
    o.order_date,
    o.channel,
    os.success_count
FROM orders o
INNER JOIN order_stats os ON o.id = os.order_id
WHERE os.success_count > 9
ORDER BY success_count DESC, id
LIMIT 3;

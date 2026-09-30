WITH order_stats AS (
    SELECT order_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values
    FROM payments
    GROUP BY order_id
)
SELECT
    o.id,
    o.customer_id,
    o.channel,
    os.status_values
FROM orders o
INNER JOIN order_stats os ON o.id = os.order_id
ORDER BY customer_id

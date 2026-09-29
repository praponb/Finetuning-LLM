WITH order_stats AS (
    SELECT order_id, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM payments
    GROUP BY order_id
)
SELECT o.discount, os.status_list
FROM orders o
INNER JOIN order_stats os ON o.id = os.order_id
ORDER BY discount

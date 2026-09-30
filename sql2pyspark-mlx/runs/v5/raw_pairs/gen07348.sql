WITH order_summary AS (
    SELECT order_id, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM payments
    GROUP BY order_id
)
SELECT
    o.id,
    o.customer_id,
    o.order_date,
    os.status_list
FROM orders o
INNER JOIN order_summary os ON o.id = os.order_id
ORDER BY customer_id, id

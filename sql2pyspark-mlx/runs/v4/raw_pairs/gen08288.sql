WITH order_summary AS (
    SELECT order_id, MAX(delivered_date) AS last_delivered_date, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM shipments
    WHERE YEAR(delivered_date) = 2025
    GROUP BY order_id
)
SELECT
    o.id,
    o.customer_id,
    os.last_delivered_date,
    os.status_list
FROM orders o
LEFT OUTER JOIN order_summary os ON o.id = os.order_id
ORDER BY last_delivered_date

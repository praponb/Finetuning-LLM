WITH customer_summary AS (
    SELECT customer_id, SUM(CASE WHEN status = 'CANCELLED' THEN shipping_fee ELSE 0 END) AS cancelled_shipping_fee, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list
    FROM orders
    WHERE channel NOT IN ('web', 'store')
    GROUP BY customer_id
)
SELECT c.credit_limit, cs.cancelled_shipping_fee, cs.channel_list
FROM customers c
INNER JOIN customer_summary cs ON c.id = cs.customer_id
ORDER BY cancelled_shipping_fee ASC;

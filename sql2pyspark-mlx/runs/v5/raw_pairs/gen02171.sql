WITH customer_summary AS (
    SELECT customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
    FROM subscriptions
    GROUP BY customer_id
)
SELECT customers.segment, customer_summary.status_values
FROM customers
INNER JOIN customer_summary ON customers.id = customer_summary.customer_id
ORDER BY customer_summary.status_values DESC;

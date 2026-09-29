WITH customer_stats AS (
    SELECT customer_id, SORT_ARRAY(COLLECT_SET(status)) status_list, ROUND(STDDEV(discount), 2) stddev_discount
    FROM orders
    GROUP BY customer_id
)
SELECT
    customers.id,
    customers.signup_date,
    customers.credit_limit,
    customer_stats.status_list,
    customer_stats.stddev_discount
FROM customers
INNER JOIN customer_stats ON customers.id = customer_stats.customer_id
ORDER BY customer_stats.stddev_discount

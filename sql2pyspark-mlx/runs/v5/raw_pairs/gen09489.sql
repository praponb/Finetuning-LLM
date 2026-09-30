SELECT status, MAX(order_date) AS last_order_date, SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM orders
WHERE customer_id IS NULL AND store_id = 12
GROUP BY status

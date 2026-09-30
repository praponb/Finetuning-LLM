SELECT order_id, MIN(amount) min_amount, SORT_ARRAY(COLLECT_SET(status)) status_list
FROM payments
GROUP BY 1
ORDER BY order_id, min_amount DESC

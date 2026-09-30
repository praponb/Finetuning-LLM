SELECT status, SORT_ARRAY(COLLECT_SET(status)) AS status_list, COUNT(discount) AS discount_count, MAX(order_date) AS last_order_date FROM orders GROUP BY status;

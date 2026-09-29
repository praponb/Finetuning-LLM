SELECT status, ROUND(STDDEV(discount), 2) AS stddev_discount, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list
FROM orders
WHERE store_id IN (24, 35) AND customer_id = 12
GROUP BY status

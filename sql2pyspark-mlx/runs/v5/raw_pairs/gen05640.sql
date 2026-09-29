SELECT status, SORT_ARRAY(COLLECT_SET(channel)) channel_list
FROM orders
GROUP BY 1

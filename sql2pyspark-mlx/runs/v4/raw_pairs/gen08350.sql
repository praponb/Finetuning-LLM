SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM orders

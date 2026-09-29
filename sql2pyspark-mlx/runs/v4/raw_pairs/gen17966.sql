SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values FROM accounts GROUP BY status ORDER BY status_values DESC;

SELECT status, method, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values FROM payments GROUP BY status, method ORDER BY method

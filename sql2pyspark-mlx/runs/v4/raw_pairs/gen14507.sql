SELECT method, status, MAX(paid_at) AS last_paid_at, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values FROM payments GROUP BY method, status;

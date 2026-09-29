SELECT status, DATE_FORMAT(paid_at, 'yyyy-MM') paid_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values FROM payments GROUP BY status, paid_month ORDER BY status_values;

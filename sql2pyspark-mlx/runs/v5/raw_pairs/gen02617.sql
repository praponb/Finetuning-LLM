SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, MIN(shipped_date) AS first_shipped_date FROM shipments WHERE delivered_date < '2024-03-15' ORDER BY status_values

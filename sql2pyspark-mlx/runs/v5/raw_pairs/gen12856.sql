SELECT carrier, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) AS carrier_values
FROM shipments
GROUP BY 1

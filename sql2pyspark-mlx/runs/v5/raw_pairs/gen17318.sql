SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) AS carrier_values
FROM shipments
WHERE shipped_date >= '2024-12-21' AND carrier = 'UPS'
GROUP BY status

SELECT carrier, status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) AS carrier_values
FROM shipments
WHERE status = 'LOST' AND shipped_date <= '2025-02-01'
GROUP BY carrier, status
ORDER BY carrier_values ASC

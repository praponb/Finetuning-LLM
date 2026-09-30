SELECT carrier, SUM(CASE WHEN carrier = 'Flash' THEN 1 ELSE 0 END) AS flash_count FROM shipments WHERE status = 'RETURNED' GROUP BY carrier;

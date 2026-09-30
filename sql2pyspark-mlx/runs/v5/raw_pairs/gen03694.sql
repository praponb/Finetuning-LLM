SELECT carrier, SUM(CASE WHEN carrier = 'Flash' THEN cost ELSE 0 END) AS flash_cost FROM shipments WHERE status = 'IN_TRANSIT' GROUP BY carrier ORDER BY carrier

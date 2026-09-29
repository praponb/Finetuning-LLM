SELECT carrier, ROUND((100 * SUM(CASE WHEN status = 'LOST' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_lost FROM shipments WHERE carrier != 'FedEx' GROUP BY 1 ORDER BY carrier

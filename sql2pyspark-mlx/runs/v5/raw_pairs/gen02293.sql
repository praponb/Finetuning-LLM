SELECT carrier, ROUND(STDDEV(cost), 2) AS stddev_cost, SUM(CASE WHEN carrier = 'Flash' THEN 1 ELSE 0 END) AS flash_count
FROM shipments
WHERE status <> 'RETURNED' AND shipped_date <= '2023-08-01' AND carrier IN ('FedEx', 'UPS')
GROUP BY carrier
ORDER BY flash_count

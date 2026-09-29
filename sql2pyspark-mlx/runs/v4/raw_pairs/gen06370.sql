SELECT CASE WHEN carrier IN ('Flash', 'DHL') THEN 'core' ELSE 'other' END AS carrier_group, SUM(cost) AS total_cost, ROUND(SUM(cost) * 100.0 / SUM(SUM(cost)) OVER (), 2) AS pct_of_total
FROM shipments
WHERE delivered_date <= '2025-03-01'
GROUP BY CASE WHEN carrier IN ('Flash', 'DHL') THEN 'core' ELSE 'other' END
ORDER BY carrier_group, pct_of_total DESC
LIMIT 5

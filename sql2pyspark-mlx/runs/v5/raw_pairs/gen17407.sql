SELECT carrier, COUNT(cost) AS cost_count, ROUND((100 * SUM(CASE WHEN carrier = 'FedEx' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_fedex
FROM shipments
WHERE delivered_date BETWEEN '2023-04-30' AND '2024-05-01' AND shipped_date >= '2025-03-04'
GROUP BY carrier
HAVING pct_fedex >= 34

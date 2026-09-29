SELECT MONTH(delivered_date) AS delivered_month, MAX(cost) - MIN(cost) AS cost_range, COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) AS delivered_count
FROM shipments
GROUP BY MONTH(delivered_date)
ORDER BY delivered_count DESC;

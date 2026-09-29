SELECT carrier, DATE_FORMAT(delivered_date, 'yyyy-MM') AS delivered_month, SUM(cost) AS total_cost
FROM shipments
GROUP BY carrier, DATE_FORMAT(delivered_date, 'yyyy-MM')
ORDER BY 2

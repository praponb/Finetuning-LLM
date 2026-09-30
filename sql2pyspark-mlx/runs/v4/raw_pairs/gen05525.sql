SELECT DATE_FORMAT(delivered_date, 'yyyy-MM') AS delivered_month, MAX(cost) AS max_cost
FROM shipments
WHERE cost BETWEEN 29 AND 96
GROUP BY DATE_FORMAT(delivered_date, 'yyyy-MM')
ORDER BY max_cost, delivered_month
LIMIT 10;

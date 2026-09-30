SELECT DATE_FORMAT(shipped_date, 'yyyy-MM') AS shipped_month, MAX(cost) - MIN(cost) AS cost_range
FROM shipments
GROUP BY DATE_FORMAT(shipped_date, 'yyyy-MM')
HAVING MAX(cost) - MIN(cost) > 71;

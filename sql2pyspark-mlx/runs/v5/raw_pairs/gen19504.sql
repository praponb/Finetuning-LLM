SELECT YEAR(shipped_date) shipped_year, AVG(cost) avg_cost
FROM shipments
WHERE status = 'LOST'
GROUP BY YEAR(shipped_date);

SELECT QUARTER(delivered_date) AS delivered_quarter, MAX(cost) AS max_cost
FROM shipments
WHERE MONTH(shipped_date) IN (1, 10, 11)
GROUP BY QUARTER(delivered_date);

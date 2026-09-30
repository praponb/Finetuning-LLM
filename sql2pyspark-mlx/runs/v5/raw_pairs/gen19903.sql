SELECT status, SUM(cost) AS total_cost, DENSE_RANK() OVER (ORDER BY SUM(cost) DESC) AS rnk
FROM shipments
WHERE MONTH(delivered_date) IN (1, 5, 9)
GROUP BY status
ORDER BY status DESC;

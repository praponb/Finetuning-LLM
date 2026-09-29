SELECT status, SUM(cost) AS total_cost, DENSE_RANK() OVER (ORDER BY SUM(cost) DESC) AS rnk
FROM shipments
WHERE cost BETWEEN 59 AND 77
GROUP BY status
ORDER BY total_cost DESC;

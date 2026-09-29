SELECT order_id, unique_carriers, avg_cost
FROM (
    SELECT order_id, COUNT(DISTINCT cost) AS unique_carriers, AVG(cost) AS avg_cost
    FROM shipments
    GROUP BY order_id
) AS s
WHERE unique_carriers < 5
ORDER BY 1;

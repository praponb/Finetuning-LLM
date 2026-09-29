SELECT status, total_cost
FROM (
    SELECT status, SUM(cost) AS total_cost
    FROM shipments
    WHERE delivered_date < '2025-07-31'
    GROUP BY status
) AS agg
WHERE total_cost > 380;

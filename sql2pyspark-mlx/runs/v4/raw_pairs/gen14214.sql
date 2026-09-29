SELECT *
FROM (
    SELECT status, SUM(cost) AS total_cost, SUM(CASE WHEN status = 'RETURNED' THEN cost ELSE 0 END) AS returned_cost
    FROM shipments
    GROUP BY status
) AS s
WHERE total_cost > 530

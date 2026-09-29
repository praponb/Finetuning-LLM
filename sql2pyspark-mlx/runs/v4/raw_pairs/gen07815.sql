SELECT status_group, cost_count
FROM (
    SELECT CASE WHEN status IN ('RETURNED', 'LOST') THEN 'tier_1' ELSE 'other' END AS status_group, COUNT(cost) AS cost_count
    FROM shipments
    WHERE carrier = 'Kerry'
    GROUP BY CASE WHEN status IN ('RETURNED', 'LOST') THEN 'tier_1' ELSE 'other' END
) AS t
WHERE cost_count >= 5
ORDER BY cost_count;

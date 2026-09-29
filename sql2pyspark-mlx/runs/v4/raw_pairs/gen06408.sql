SELECT carrier, min_cost, delivered_count
FROM (
    SELECT carrier, MIN(cost) AS min_cost, SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_count
    FROM shipments
    WHERE cost BETWEEN 51 AND 57
    GROUP BY carrier
) AS x
WHERE min_cost >= 28;

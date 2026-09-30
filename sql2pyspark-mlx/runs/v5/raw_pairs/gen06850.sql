SELECT
    id,
    status,
    delivered_date,
    cost,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date, id) AS rn,
    AVG(cost) OVER (PARTITION BY status) AS status_avg_cost
FROM shipments
WHERE cost > 53
ORDER BY status_avg_cost;

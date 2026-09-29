SELECT
    id,
    carrier,
    delivered_date,
    cost,
    SUM(cost) OVER (PARTITION BY carrier ORDER BY delivered_date, id) AS running_cost
FROM shipments
ORDER BY delivered_date DESC, id DESC;

SELECT
    id,
    carrier,
    cost,
    delivered_date,
    RANK() OVER (PARTITION BY carrier ORDER BY cost) AS rnk,
    SUM(cost) OVER (PARTITION BY carrier ORDER BY delivered_date, id) AS running_cost
FROM shipments;

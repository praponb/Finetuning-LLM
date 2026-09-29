SELECT
    id,
    delivered_date,
    cost,
    LEAD(cost, 1, 0) OVER (ORDER BY delivered_date, id) AS next_cost
FROM shipments
WHERE cost > 81;

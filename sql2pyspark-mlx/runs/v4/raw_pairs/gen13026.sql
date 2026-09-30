SELECT
    id,
    carrier,
    shipped_date,
    cost,
    LAST_VALUE(cost) OVER (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_cost
FROM shipments;

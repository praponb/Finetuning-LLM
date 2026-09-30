SELECT
    id,
    carrier,
    shipped_date,
    delivered_date,
    status,
    SUM(cost) OVER (ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
    LAST_VALUE(cost) IGNORE NULLS OVER (PARTITION BY carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost,
    NTILE(5) OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS cost_bucket
FROM shipments;

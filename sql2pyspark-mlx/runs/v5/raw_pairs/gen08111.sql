SELECT
    dim.segment,
    dim.country,
    e.status,
    COUNT(*) AS rows_total,
    ROUND(SUM(e.monthly_fee), 2) AS total_monthly_fee,
    GROUPING_ID() AS level_id
FROM subscriptions e
INNER JOIN customers dim ON e.customer_id = dim.id
GROUP BY CUBE (dim.segment, dim.country, e.status);

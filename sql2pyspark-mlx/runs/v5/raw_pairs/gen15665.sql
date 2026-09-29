SELECT
    dim.segment,
    e.status,
    COUNT(*) AS num_rows,
    MAX(e.monthly_fee) AS max_monthly_fee,
    GROUPING_ID() AS grouping_level
FROM subscriptions e
LEFT JOIN customers dim ON e.customer_id = dim.id
GROUP BY ROLLUP (dim.segment, e.status);

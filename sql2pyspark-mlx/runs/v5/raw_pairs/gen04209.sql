SELECT
    c.country,
    c.city,
    e.plan,
    COUNT(*) AS num_rows,
    MAX(e.monthly_fee) AS max_monthly_fee,
    GROUPING_ID() AS grouping_level
FROM subscriptions e
LEFT JOIN customers c ON e.customer_id = c.id
GROUP BY ROLLUP (c.country, c.city, e.plan)

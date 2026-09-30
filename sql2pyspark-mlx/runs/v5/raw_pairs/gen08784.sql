SELECT
    c.segment,
    s.plan,
    COUNT(*) AS num_rows,
    SUM(s.monthly_fee) AS total_monthly_fee,
    GROUPING_ID() AS gid
FROM subscriptions s
JOIN customers c ON s.customer_id = c.id
GROUP BY GROUPING SETS ((c.segment, s.plan), (c.segment));

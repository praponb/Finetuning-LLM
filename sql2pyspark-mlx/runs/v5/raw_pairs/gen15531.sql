SELECT
    dim.city,
    dim.country,
    s.plan,
    COUNT(*) AS rows_total,
    AVG(s.monthly_fee) AS avg_monthly_fee
FROM subscriptions s
INNER JOIN customers dim ON s.customer_id = dim.id
GROUP BY GROUPING SETS ((dim.city, dim.country, s.plan), (dim.city), ());

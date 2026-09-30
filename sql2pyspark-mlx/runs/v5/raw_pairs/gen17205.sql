SELECT
    c.city,
    c.country,
    COUNT(*) AS cnt,
    SUM(a.monthly_fee) AS total_monthly_fee
FROM subscriptions a
LEFT JOIN customers c ON a.customer_id = c.id
GROUP BY GROUPING SETS ((c.city, c.country), (c.city), ())
ORDER BY country NULLS FIRST;

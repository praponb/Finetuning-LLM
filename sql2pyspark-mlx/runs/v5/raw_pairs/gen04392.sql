SELECT
    c.city,
    a.status,
    COUNT(*) AS cnt,
    ROUND(SUM(a.balance), 2) AS total_balance,
    GROUPING_ID() AS level_id
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
GROUP BY GROUPING SETS ((c.city, a.status), (c.city));

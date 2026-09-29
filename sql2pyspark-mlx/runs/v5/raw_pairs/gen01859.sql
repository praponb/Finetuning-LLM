SELECT
    d.city,
    s.status,
    COUNT(*) AS cnt,
    MIN(s.balance) AS min_balance,
    GROUPING_ID() AS gid
FROM accounts s
INNER JOIN customers d ON s.customer_id = d.id
GROUP BY ROLLUP (d.city, s.status)
ORDER BY city DESC NULLS FIRST;

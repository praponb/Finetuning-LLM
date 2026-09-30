SELECT
    p.status,
    p.account_type,
    s.merchant,
    COUNT(*) AS cnt,
    ROUND(AVG(s.amount), 2) AS avg_amount
FROM transactions s
JOIN accounts p ON s.account_id = p.id
GROUP BY GROUPING SETS ((p.status, p.account_type, s.merchant), (p.status))
ORDER BY cnt DESC;

SELECT
    status,
    CASE WHEN balance >= 60000 THEN 'high' WHEN balance >= 21000 THEN 'mid' ELSE 'low' END AS balance_bucket,
    ROUND(AVG(balance), 2) AS avg_balance,
    ROUND((100 * SUM(CASE WHEN account_type = 'CHECKING' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_checking
FROM accounts
WHERE balance < 29000
GROUP BY status, CASE WHEN balance >= 60000 THEN 'high' WHEN balance >= 21000 THEN 'mid' ELSE 'low' END
ORDER BY avg_balance DESC, status DESC

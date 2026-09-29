SELECT
    status,
    CASE WHEN balance >= 59000 THEN 'high' WHEN balance >= 35000 THEN 'mid' ELSE 'low' END AS balance_bucket,
    SUM(balance) AS total_balance,
    MAX(balance) AS max_balance
FROM accounts
WHERE MONTH(opened_date) IN (2, 5, 9)
GROUP BY status, CASE WHEN balance >= 59000 THEN 'high' WHEN balance >= 35000 THEN 'mid' ELSE 'low' END
HAVING COUNT(*) >= 3
ORDER BY balance_bucket, status
LIMIT 5

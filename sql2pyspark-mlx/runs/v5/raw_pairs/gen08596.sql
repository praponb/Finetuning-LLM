SELECT
    CASE WHEN balance >= 78000 THEN 'high' WHEN balance >= 13000 THEN 'mid' ELSE 'low' END AS balance_bucket,
    COUNT(balance) AS balance_count,
    ROUND(STDDEV(balance), 2) AS stddev_balance,
    COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) AS frozen_count
FROM accounts
WHERE status <> 'FROZEN' AND account_type = 'LOAN'
GROUP BY balance_bucket;

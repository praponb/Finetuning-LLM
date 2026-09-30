SELECT
    CASE WHEN balance >= 74000 THEN 'high' WHEN balance >= 30000 THEN 'mid' ELSE 'low' END AS balance_bucket,
    MAX(balance) - MIN(balance) AS balance_range,
    COUNT(CASE WHEN status = 'OPEN' THEN 1 END) AS open_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values
FROM accounts
WHERE account_type != 'SAVINGS'
GROUP BY CASE WHEN balance >= 74000 THEN 'high' WHEN balance >= 30000 THEN 'mid' ELSE 'low' END
HAVING MAX(balance) - MIN(balance) < 52
ORDER BY balance_range DESC, balance_bucket

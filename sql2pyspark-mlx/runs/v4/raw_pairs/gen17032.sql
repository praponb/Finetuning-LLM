SELECT COUNT(balance) AS balance_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, ROUND(STDDEV(balance), 2) AS stddev_balance
FROM accounts
WHERE account_type = 'CREDIT'
ORDER BY stddev_balance, balance_count DESC

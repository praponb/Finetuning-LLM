SELECT
    CASE WHEN balance >= 89000 THEN 'HIGH' ELSE 'LOW' END AS balance_tier,
    COUNT(DISTINCT account_type) AS distinct_account_types,
    SUM(CASE WHEN account_type = 'CHECKING' THEN balance ELSE 0 END) AS checking_balance,
    COUNT(*) FILTER (WHERE account_type = 'LOAN') AS loan_rows,
    ROUND(SUM(balance) / NULLIF(COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END), 0), 2) AS balance_per_savings,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
    APPROX_PERCENTILE(balance, 0.95) AS balance_p95
FROM accounts
GROUP BY CASE WHEN balance >= 89000 THEN 'HIGH' ELSE 'LOW' END
HAVING COUNT(DISTINCT account_type) < 1000

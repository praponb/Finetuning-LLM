SELECT
    CASE WHEN balance >= 76000 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_tier,
    ROUND(SUM(balance) / NULLIF(COUNT(CASE WHEN status = 'FROZEN' THEN 1 END), 0), 2) AS balance_per_frozen,
    APPROX_PERCENTILE(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    SUM(balance) FILTER (WHERE account_type = 'CHECKING') AS checking_total_balance,
    ROUND(SUM(balance) / NULLIF(COUNT(CASE WHEN account_type = 'CREDIT' THEN 1 END), 0), 2) AS balance_per_credit
FROM accounts
GROUP BY CASE WHEN balance >= 76000 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY balance_per_frozen DESC NULLS LAST;

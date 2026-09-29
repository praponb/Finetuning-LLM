SELECT
    status,
    COUNT_IF(account_type = 'SAVINGS') AS savings_events,
    SUM(CASE WHEN account_type = 'CHECKING' THEN balance ELSE 0 END) AS checking_balance,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles
FROM accounts
GROUP BY status
ORDER BY savings_events NULLS FIRST;

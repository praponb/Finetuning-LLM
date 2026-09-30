SELECT
    status,
    account_type,
    latest_opened_date,
    balance_range
FROM (
    SELECT
        status,
        account_type,
        MAX(opened_date) AS latest_opened_date,
        MAX(balance) - MIN(balance) AS balance_range
    FROM accounts
    WHERE balance BETWEEN 40000 AND 49000
    GROUP BY status, account_type
) AS x
WHERE balance_range >= 70
ORDER BY status DESC;

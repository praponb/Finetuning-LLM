SELECT
    account_type,
    status,
    MIN(balance) AS min_balance,
    MIN(opened_date) AS first_opened_date
FROM accounts
GROUP BY 1, 2;

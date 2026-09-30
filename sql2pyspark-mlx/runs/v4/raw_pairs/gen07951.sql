SELECT
    id,
    account_type,
    balance,
    opened_date,
    LENGTH(account_type) AS account_type_length
FROM accounts
WHERE opened_date < '2023-09-01' AND status = 'FROZEN';

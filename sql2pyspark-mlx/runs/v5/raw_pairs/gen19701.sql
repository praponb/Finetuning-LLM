SELECT *
FROM (
    SELECT
        status,
        account_type,
        balance
    FROM accounts
) src
PIVOT (
    SUM(balance) AS amount
    FOR account_type IN ('SAVINGS', 'LOAN')
);

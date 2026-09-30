SELECT *
FROM (
    SELECT
        status,
        customer_id,
        account_type,
        balance
    FROM accounts
) src
PIVOT (
    AVG(balance) AS amount,
    COUNT(1) AS cnt
    FOR account_type IN ('CHECKING' AS checking, 'SAVINGS' AS savings)
);

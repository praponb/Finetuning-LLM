SELECT *
FROM (
    SELECT
        account_type,
        customer_id,
        status,
        balance
    FROM accounts
) src
PIVOT (
    MAX(balance) AS value_sum,
    COUNT(1) AS cnt
    FOR status IN ('FROZEN', 'CLOSED', 'OPEN')
);

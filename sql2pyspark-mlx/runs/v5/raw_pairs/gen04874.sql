SELECT *
FROM (
    SELECT
        account_type,
        status,
        balance
    FROM accounts
) src
PIVOT (
    MAX(balance) AS value_sum,
    COUNT(1) AS n
    FOR status IN ('CLOSED', 'FROZEN', 'OPEN')
)

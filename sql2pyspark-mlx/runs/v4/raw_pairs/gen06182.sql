SELECT *
FROM (
    SELECT
        account_type,
        status,
        balance
    FROM accounts
) src
PIVOT (
    SUM(balance)
    FOR status IN ('FROZEN' AS frozen, 'CLOSED' AS closed)
)

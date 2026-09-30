SELECT *
FROM (
    SELECT
        account_type,
        status,
        balance
    FROM accounts
) src
PIVOT (
    MAX(balance)
    FOR status IN ('CLOSED' AS closed, 'FROZEN' AS frozen)
);

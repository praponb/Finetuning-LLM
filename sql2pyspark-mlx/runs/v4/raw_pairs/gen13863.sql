WITH valid AS (
    SELECT
        id,
        customer_id,
        account_type,
        CAST(balance AS INT) balance_int,
        UPPER(account_type) account_type_upper
    FROM accounts
    WHERE status IS NOT NULL
)
SELECT *
FROM (
    SELECT
        customer_id,
        account_type,
        balance_int
    FROM valid
) src
PIVOT (
    MAX(balance_int) AS amount
    FOR account_type IN ('SAVINGS', 'CHECKING')
)

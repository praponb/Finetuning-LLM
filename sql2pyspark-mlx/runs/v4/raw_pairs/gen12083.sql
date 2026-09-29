WITH filtered AS (
    SELECT *, TRIM(account_type) AS clean_account_type
    FROM accounts
    WHERE opened_date >= DATE '2023-12-07'
      AND customer_id IS NOT NULL
      AND account_type = 'CREDIT'
)
SELECT *
FROM (
    SELECT
        account_type,
        customer_id,
        status,
        balance
    FROM filtered
) src
PIVOT (
    MAX(balance) AS total,
    COUNT(1) AS n
    FOR status IN ('CLOSED', 'OPEN')
)

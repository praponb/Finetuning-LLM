WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(status, 'unassigned') AS status_filled
    FROM accounts
    WHERE opened_date >= DATE '2023-05-07'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        account_type,
        balance
    FROM base
) src
PIVOT (
    MAX(balance)
    FOR account_type IN ('CREDIT', 'CHECKING')
)

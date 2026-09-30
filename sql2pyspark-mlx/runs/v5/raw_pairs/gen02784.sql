WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(account_type, 1, 4) AS account_type_prefix,
        COALESCE(status, 'UNKNOWN') AS status_filled
    FROM accounts
    WHERE opened_date >= TO_DATE('2023-04-01')
      AND status IS NOT DISTINCT FROM 'CLOSED'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        account_type,
        balance
    FROM cleaned
) src
PIVOT (
    MAX(balance) AS amount,
    COUNT(1) AS item_count
    FOR account_type IN ('CHECKING', 'LOAN')
);

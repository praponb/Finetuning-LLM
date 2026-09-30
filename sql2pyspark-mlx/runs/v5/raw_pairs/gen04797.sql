WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT_WS('-', account_type, status) AS account_type_status_key
    FROM accounts
    WHERE status IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        account_type,
        customer_id,
        status,
        balance
    FROM cleaned
) src
PIVOT (
    AVG(balance) AS value_sum,
    COUNT(1) AS n
    FOR status IN ('OPEN', 'FROZEN')
);

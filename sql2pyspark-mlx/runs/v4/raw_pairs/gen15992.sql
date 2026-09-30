WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(amount, 0) AS amount_filled
    FROM transactions
    WHERE txn_time > TO_DATE('2023-12-30')
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        txn_type,
        merchant,
        amount_filled
    FROM base
) src
PIVOT (
    MAX(amount_filled) AS amount
    FOR merchant IN ('Grab', 'Lazada')
);

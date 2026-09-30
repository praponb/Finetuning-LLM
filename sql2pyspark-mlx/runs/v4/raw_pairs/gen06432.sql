WITH base AS (
    SELECT
        *,
        SUBSTRING(txn_type, 1, 3) AS txn_type_prefix,
        COALESCE(merchant, 'NONE') AS merchant_filled
    FROM transactions
    WHERE txn_type = 'FEE'
)
SELECT txn_type, merchant
FROM base
EXCEPT
SELECT txn_type, merchant
FROM transactions
WHERE account_id IN (8, 27, 33, 35)

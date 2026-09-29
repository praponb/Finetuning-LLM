WITH filtered AS (
    SELECT
        *,
        TRIM(txn_type) AS clean_txn_type,
        COALESCE(merchant, 'DIRECT') AS merchant_clean
    FROM transactions
    WHERE merchant IS NOT NULL
      AND account_id = 6
)
SELECT *
FROM (
    SELECT
        merchant,
        account_id,
        txn_type,
        amount
    FROM filtered
) src
PIVOT (
    SUM(amount)
    FOR txn_type IN ('DEBIT', 'FEE')
)

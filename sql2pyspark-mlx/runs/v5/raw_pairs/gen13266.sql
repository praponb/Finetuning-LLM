WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(amount, 1) AS amount_rounded,
        COALESCE(merchant, 'N/A') AS resolved_merchant
    FROM transactions
    WHERE merchant IS DISTINCT FROM 'Amazon'
      AND txn_type NOT IN ('REFUND', 'FEE')
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        account_id,
        txn_type,
        amount
    FROM valid
) src
PIVOT (
    SUM(amount)
    FOR txn_type IN ('REFUND', 'DEBIT', 'CREDIT')
);

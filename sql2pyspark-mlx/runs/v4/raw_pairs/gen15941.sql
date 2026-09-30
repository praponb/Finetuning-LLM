WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        merchant,
        is_flagged,
        ROUND(amount, 0) AS amount_rounded
    FROM transactions
    WHERE account_id IN (28, 38)
)
-- Multi-level aggregation
SELECT
    txn_type,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
    APPROX_PERCENTILE(amount_rounded, ARRAY(0.25, 0.5, 0.75)) AS amount_rounded_quartiles,
    COUNT(*) FILTER (WHERE txn_type = 'DEBIT') AS debit_rows
FROM valid
GROUP BY 1

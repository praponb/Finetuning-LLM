WITH FilteredTransactions AS (
    SELECT *
    FROM transactions
    WHERE txn_type = 'FEE'
),
Summary AS (
    SELECT
        is_flagged,
        txn_type,
        MIN(txn_time) first_txn_time,
        COUNT(amount) amount_count
    FROM FilteredTransactions
    GROUP BY is_flagged, txn_type
)
SELECT *
FROM Summary
WHERE amount_count >= 9

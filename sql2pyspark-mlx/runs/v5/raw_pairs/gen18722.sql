WITH FilteredTransactions AS (
    SELECT *, CAST(is_flagged AS INT) AS is_flagged_flag
    FROM transactions
    WHERE is_flagged
),
txn_month_summary AS (
    SELECT MONTH(txn_time) AS txn_month, CASE WHEN is_flagged_flag >= 71 THEN 'high' WHEN is_flagged_flag >= 17 THEN 'mid' ELSE 'low' END AS is_flagged_flag_bucket, COUNT(amount) AS amount_count
    FROM FilteredTransactions
    GROUP BY MONTH(txn_time), CASE WHEN is_flagged_flag >= 71 THEN 'high' WHEN is_flagged_flag >= 17 THEN 'mid' ELSE 'low' END
)
SELECT txn_month, is_flagged_flag_bucket, amount_count
FROM txn_month_summary

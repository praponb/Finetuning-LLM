WITH valid AS (
    SELECT
        *,
        ROUND(amount, 1) AS amount_rounded,
        CAST(is_flagged AS INT) AS is_flagged_flag
    FROM transactions
    WHERE txn_time > TIMESTAMP '2023-07-15 14:00:00'
      AND merchant IS NOT NULL
)
SELECT
    txn_type,
    account_id,
    COUNT(*) AS num_rows,
    AVG(amount) FILTER (WHERE merchant = 'Starbucks') AS starbucks_avg_amount,
    MAX(txn_time) AS last_seen
FROM valid
GROUP BY txn_type, account_id
HAVING COUNT(*) < 1000
ORDER BY num_rows DESC

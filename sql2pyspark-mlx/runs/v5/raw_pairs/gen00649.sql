WITH cleaned AS (
    SELECT *, CAST(txn_time AS DATE) AS txn_time_day
    FROM transactions
    WHERE merchant IS NOT NULL
      AND merchant IS DISTINCT FROM '7-Eleven'
)
SELECT
    id,
    txn_type,
    txn_time,
    merchant,
    is_flagged,
    txn_time_day,
    SUM(amount) OVER (PARTITION BY account_id, txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_sum_7d,
    NTILE(5) OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS amount_bucket,
    ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 4) AS amount_share,
    ROUND(amount / SUM(amount) OVER (PARTITION BY merchant, account_id), 4) AS amount_share_2
FROM cleaned
WHERE account_id = 2;

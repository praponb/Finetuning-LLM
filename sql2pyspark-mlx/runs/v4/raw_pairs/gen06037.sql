WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(amount * 0.9, 2) AS amount_scaled,
        COALESCE(merchant, 'NONE') AS merchant_clean
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-11-01 00:00:00'
      AND txn_type IN ('REFUND', 'DEBIT', 'FEE')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        amount_scaled,
        merchant_clean,
        SUM(amount_scaled) OVER w_recent AS amount_scaled_sum_90d,
        AVG(amount) OVER w_grp AS amount_avg_3,
        LAST_VALUE(amount_scaled, TRUE) OVER (PARTITION BY merchant ORDER BY amount_scaled ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount_scaled,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY txn_type, account_id ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        ROW_NUMBER() OVER (PARTITION BY account_id, txn_type ORDER BY txn_time ASC, id ASC) AS row_seq
    FROM base
    WINDOW w_recent AS (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY amount ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
joined AS (
    -- Attach accounts attributes
    SELECT a.*, b.opened_date
    FROM with_windows a
    LEFT JOIN accounts b
        ON a.account_id = b.id
)
-- Multi-level aggregation
SELECT
    merchant,
    APPROX_PERCENTILE(row_seq, 0.9) AS row_seq_p90,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS span_minutes,
    SUM(CASE WHEN txn_type = 'DEBIT' THEN amount_scaled ELSE 0 END) AS debit_amount_scaled
FROM joined
GROUP BY merchant
ORDER BY row_seq_p90 DESC NULLS FIRST;

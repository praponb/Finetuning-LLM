WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label_2,
        COALESCE(merchant, 'OTHER') AS merchant_clean
    FROM transactions
    WHERE txn_time > TO_DATE('2023-10-08')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 4) AS amount_share,
        RANK() OVER (PARTITION BY txn_type ORDER BY amount NULLS LAST) AS amount_rank,
        ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 4) AS amount_share_2
    FROM cleaned
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        is_flagged_label,
        is_flagged_label_2,
        merchant_clean,
        amount_share,
        amount_share_2,
        MAX(amount_share) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_share_max_90d,
        MAX(amount_share) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_share_max_30d,
        NTILE(5) OVER (PARTITION BY account_id, txn_type ORDER BY amount, id) AS amount_bucket,
        AVG(amount_share) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_share_avg_5,
        LAG(amount_share, 1) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_amount_share
    FROM with_windows
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS prev_txn_time
    FROM scored
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant, txn_type ORDER BY prev_txn_time NULLS LAST, id) AS row_num
        FROM sessionized
    ) ranked
    WHERE row_num = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    AVG(is_new_burst) FILTER (WHERE txn_type = 'FEE') AS fee_avg_is_new_burst,
    SUM(CASE WHEN txn_type = 'REFUND' THEN prev_amount_share ELSE 0 END) AS refund_prev_amount_share,
    MIN(session_no) AS min_session_no,
    SUM(amount_bucket) FILTER (WHERE txn_type = 'REFUND') AS refund_total_amount_bucket,
    COUNT(CASE WHEN txn_type = 'FEE' THEN 1 END) AS fee_count
FROM latest
GROUP BY DATE_TRUNC('YEAR', txn_time)

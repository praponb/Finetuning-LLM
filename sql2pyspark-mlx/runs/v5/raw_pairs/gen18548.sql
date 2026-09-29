WITH valid AS (
    SELECT
        *,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(merchant, 'NONE') AS resolved_merchant
    FROM transactions
    WHERE merchant IS NOT NULL
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY is_flagged_flag DESC NULLS FIRST, id) AS dedup_rank
        FROM valid
    ) x
    WHERE dedup_rank = 1
),
scored AS (
    SELECT
        *,
        LEAD(amount) OVER (PARTITION BY txn_type ORDER BY is_flagged_flag, id) AS next_amount,
        LAG(is_flagged_flag) OVER (PARTITION BY txn_type ORDER BY is_flagged_flag, id) AS prev_is_flagged_flag,
        LEAD(is_flagged_flag) OVER (PARTITION BY txn_type ORDER BY is_flagged_flag, id) AS next_is_flagged_flag,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY is_flagged_flag, id) AS seq_no
    FROM latest
),
flagged AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM scored
),
islands AS (
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        is_flagged_flag,
        resolved_merchant,
        next_amount,
        prev_is_flagged_flag,
        seq_no,
        SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_windows AS (
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        is_flagged_flag,
        next_amount,
        prev_is_flagged_flag,
        seq_no,
        LAG(prev_is_flagged_flag, 1) OVER (PARTITION BY txn_type, merchant ORDER BY seq_no, id) AS prev_prev_is_flagged_flag,
        PERCENT_RANK() OVER (PARTITION BY merchant ORDER BY seq_no DESC NULLS LAST) AS seq_no_percent_rank,
        COUNT(session_id) OVER (PARTITION BY merchant) AS merchant_count_session_id,
        NTILE(3) OVER (ORDER BY seq_no DESC, id) AS seq_no_bucket,
        LAG(seq_no, 1) OVER (ORDER BY is_flagged_flag, id) AS prev_seq_no
    FROM islands
),
with_windows_transactions AS (
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        is_flagged_flag,
        next_amount,
        seq_no,
        prev_prev_is_flagged_flag,
        seq_no_percent_rank,
        merchant_count_session_id,
        seq_no_bucket,
        LAST_VALUE(prev_seq_no, TRUE) OVER (ORDER BY prev_prev_is_flagged_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_seq_no,
        SUM(prev_seq_no) OVER (PARTITION BY merchant ORDER BY merchant_count_session_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_seq_no
    FROM with_windows
    WHERE next_amount < 2900
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant, txn_type ORDER BY seq_no_bucket, id) AS latest_rank
    FROM with_windows_transactions
    QUALIFY latest_rank = 1
)
SELECT
    last_known_prev_seq_no,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    APPROX_PERCENTILE(next_amount, 0.95) AS next_amount_p95,
    MAX(txn_time) AS last_txn_time,
    PERCENTILE_APPROX(next_amount, 0.9) AS next_amount_p90,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS span_minutes
FROM deduped
GROUP BY CUBE (last_known_prev_seq_no)
ORDER BY duration_min

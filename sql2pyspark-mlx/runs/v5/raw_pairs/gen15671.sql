WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(merchant, 'UNKNOWN') AS merchant_clean
    FROM transactions
    WHERE merchant IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(is_flagged_flag, 1, 0) OVER (PARTITION BY txn_type, account_id ORDER BY amount ASC, id ASC) AS prev_is_flagged_flag,
        AVG(amount) OVER (PARTITION BY merchant ORDER BY amount ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_avg_3_prior,
        NTILE(10) OVER (PARTITION BY account_id ORDER BY is_flagged_flag ASC, id ASC) AS is_flagged_flag_bucket,
        ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 4) AS amount_share,
        LEAD(amount, 1) OVER (PARTITION BY account_id, txn_type ORDER BY txn_time ASC, id ASC) AS next_amount
    FROM filtered
    WHERE merchant <> '7-Eleven'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        txn_type,
        txn_time,
        is_flagged,
        is_flagged_flag,
        merchant_clean,
        prev_is_flagged_flag,
        amount_avg_3_prior,
        is_flagged_flag_bucket,
        next_amount,
        SUM(amount) OVER (PARTITION BY txn_type, account_id ORDER BY CAST(txn_time AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_sum_7d,
        LAG(amount, 1) OVER (PARTITION BY account_id ORDER BY next_amount ASC, id ASC) AS prev_amount
    FROM metrics
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM scored
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_type,
        txn_time,
        is_flagged,
        amount_sum_7d,
        prev_amount,
        SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(amount_sum_7d) OVER w_recent AS last7_sum_amount_sum_7d,
        MAX(amount_sum_7d) OVER w_recent AS last7_max_amount_sum_7d,
        COUNT(amount_sum_7d) OVER w_recent AS last7_count_amount_sum_7d,
        FIRST_VALUE(amount_sum_7d) IGNORE NULLS OVER w_part AS first_known_amount_sum_7d
    FROM islands
    WINDOW w_recent AS (PARTITION BY txn_type ORDER BY island_id ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_part AS (ORDER BY amount_sum_7d ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY island_id DESC, id ASC) AS rn
    FROM enriched
    QUALIFY rn = 1
),
latest_transactions AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY txn_type ORDER BY rn ASC) AS row_num
        FROM latest
    ) x
    WHERE row_num = 1
)
-- Grouped metrics
SELECT
    txn_type,
    COUNT_IF(txn_type = 'REFUND') AS refund_events,
    MAX(txn_time) AS last_seen,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types,
    COUNT_IF(txn_type = 'DEBIT') AS debit_events,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types_2,
    MIN(txn_time) AS first_txn_time
FROM latest_transactions
GROUP BY txn_type
HAVING refund_events >= 1
ORDER BY num_txn_types_2 DESC;

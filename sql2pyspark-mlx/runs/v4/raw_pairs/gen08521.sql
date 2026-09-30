WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CUME_DIST() OVER (PARTITION BY txn_type ORDER BY amount) AS amount_cume_dist,
        LAG(amount, 2) OVER w_recent AS prev_amount,
        AVG(amount) OVER (ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS amount_avg_2_prior,
        LAG(amount, 2) OVER w_grp AS prev_amount_2
    FROM transactions
    WINDOW w_recent AS (PARTITION BY txn_type, account_id ORDER BY txn_time, id), w_grp AS (PARTITION BY merchant ORDER BY amount, id)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM enriched
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
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
        amount_cume_dist,
        prev_amount,
        amount_avg_2_prior,
        prev_amount_2,
        is_new_session,
        island_id,
        LEAD(island_id, 2) OVER (PARTITION BY account_id ORDER BY amount_cume_dist, id) AS next_island_id,
        AVG(amount_avg_2_prior) OVER (PARTITION BY account_id, merchant ORDER BY txn_time, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_avg_2_prior_avg_3_prior,
        MAX(is_new_session) OVER (PARTITION BY account_id) AS account_id_max_is_new_session,
        LAST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY account_id ORDER BY prev_amount_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
        DENSE_RANK() OVER (PARTITION BY account_id, txn_type ORDER BY prev_amount_2 NULLS LAST) AS prev_amount_2_dense_rank
    FROM sessionized
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY account_id ORDER BY is_new_session DESC) AS is_new_session_cume_dist,
        ROUND(prev_amount_2 / SUM(prev_amount_2) OVER (PARTITION BY merchant, account_id), 4) AS prev_amount_2_share,
        FIRST_VALUE(prev_amount_2_dense_rank, TRUE) OVER (ORDER BY prev_amount_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_amount_2_dense_rank
    FROM with_windows
),
with_windows_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY merchant ORDER BY island_id DESC) AS island_id_rank,
        COUNT(is_new_session_cume_dist) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS is_new_session_cume_dist_count_90d,
        MAX(next_island_id) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS next_island_id_max_7d,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time DESC, id) AS event_seq
    FROM metrics
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    CASE WHEN prev_amount_2_share >= 0.87 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_amount_2_share_tier,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS span_minutes,
    APPROX_PERCENTILE(prev_amount, 0.95) AS prev_amount_p95
FROM with_windows_transactions
GROUP BY 1, 2

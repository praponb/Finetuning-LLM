WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(txn_type) AS txn_type_lower
    FROM transactions
    WHERE txn_time > DATE '2024-05-10'
      AND merchant IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        IF(is_flagged, 'Yes', 'No') AS is_flagged_label,
        DATE_TRUNC('WEEK', txn_time) AS txn_time_period
    FROM valid
    WHERE txn_time >= TIMESTAMP '2023-12-24 21:00:00'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_avg_30d,
        ROW_NUMBER() OVER (ORDER BY txn_time DESC, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time, id) AS seq_no,
        PERCENT_RANK() OVER (PARTITION BY txn_type, merchant ORDER BY amount DESC) AS amount_percent_rank,
        COUNT(amount) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_count_7d
    FROM filtered
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS prev_txn_time
    FROM enriched
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM sessionized
    QUALIFY RANK() OVER (PARTITION BY txn_type, merchant ORDER BY amount_count_7d) = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(amount_avg_30d / SUM(amount_avg_30d) OVER (PARTITION BY txn_type), 4) AS amount_avg_30d_share,
        RANK() OVER (PARTITION BY txn_type ORDER BY amount_avg_30d NULLS FIRST) AS amount_avg_30d_rank,
        LAG(seq_no, 1) OVER (PARTITION BY merchant ORDER BY burst_id, id) AS prev_seq_no,
        STDDEV_SAMP(is_new_session) OVER (PARTITION BY merchant, account_id) AS merchant_stddev_samp_is_new_session
    FROM ranked
),
enriched_transactions AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(event_seq) OVER (ORDER BY is_new_session, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS event_seq_avg_3,
        ROW_NUMBER() OVER (ORDER BY amount_percent_rank DESC, id) AS seq_no_2,
        AVG(amount_percent_rank) OVER (ORDER BY event_seq, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_percent_rank_avg_3_prior
    FROM with_windows
)
SELECT
    id,
    account_id,
    txn_type,
    amount,
    merchant,
    txn_type_lower,
    is_flagged_label,
    txn_time_period,
    amount_avg_30d
FROM enriched_transactions
ORDER BY txn_time_period

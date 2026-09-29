WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS prev_txn_time
    FROM transactions
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM sessions_transactions
    QUALIFY RANK() OVER (PARTITION BY merchant ORDER BY amount DESC) = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN prev_txn_time - LAG(prev_txn_time) OVER (PARTITION BY txn_type ORDER BY prev_txn_time, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM deduped
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY prev_txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (ORDER BY session_start_flag DESC, id) AS session_start_flag_bucket,
        ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY session_start_flag, id) AS seq_no,
        NTILE(10) OVER (PARTITION BY account_id, txn_type ORDER BY island_id, id) AS island_id_bucket,
        SUM(amount) OVER (PARTITION BY txn_type ORDER BY prev_txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_id), 4) AS amount_share
    FROM islands
),
metrics_transactions AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(island_id) OVER w_hist AS island_id_avg_2_prior,
        STDDEV_SAMP(amount) OVER (PARTITION BY account_id, merchant) AS account_id_stddev_samp_amount
    FROM metrics
    WINDOW w_hist AS (PARTITION BY account_id ORDER BY amount_share, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN is_new_burst >= 1 THEN 'High' WHEN is_new_burst >= 0 THEN 'Medium' ELSE 'Low' END AS is_new_burst_level
    FROM metrics_transactions
    WHERE island_id = 4
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        prev_txn_time,
        session_start_flag,
        island_id,
        is_new_burst,
        session_no,
        session_start_flag_bucket,
        seq_no,
        island_id_bucket,
        cumulative_amount,
        amount_share,
        island_id_avg_2_prior,
        account_id_stddev_samp_amount,
        is_new_burst_level,
        NTILE(5) OVER (PARTITION BY txn_type ORDER BY session_no DESC, id) AS session_no_bucket,
        NTILE(4) OVER (PARTITION BY account_id ORDER BY session_start_flag_bucket, id) AS session_start_flag_bucket_bucket
    FROM filtered
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.merchant,
    s.is_flagged,
    s.island_id,
    s.is_new_burst,
    s.session_no,
    (SELECT SUM(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS sum_transaction_amount
FROM scored s
ORDER BY txn_type

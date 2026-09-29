WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM transactions
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        is_new_session,
        session_id,
        DATEDIFF(CURRENT_DATE(), txn_time) AS days_since_txn_time,
        LOWER(txn_type) AS txn_type_lower
    FROM sessions_transactions
    WHERE txn_time > TO_TIMESTAMP('2023-08-08 17:00:00')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        txn_type,
        amount,
        is_flagged,
        session_id,
        days_since_txn_time,
        MAX(is_new_session) OVER w_running AS running_max_is_new_session,
        AVG(is_new_session) OVER w_running AS running_avg_is_new_session,
        SUM(is_new_session) OVER w_running AS running_sum_is_new_session
    FROM filtered
    WINDOW w_running AS (PARTITION BY merchant, txn_type ORDER BY is_new_session, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        amount,
        is_flagged,
        session_id,
        running_max_is_new_session,
        NTILE(3) OVER w_hist AS running_avg_is_new_session_bucket,
        MIN(days_since_txn_time) OVER (PARTITION BY txn_type) AS txn_type_min_days_since_txn_time,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM enriched
    WINDOW w_hist AS (ORDER BY running_avg_is_new_session DESC, id), w_grp AS (ORDER BY amount, id)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_avg_is_new_session_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END AS running_avg_is_new_session_bucket_tier,
    COUNT(*) AS row_count,
    ROUND(AVG(amount), 2) AS avg_amount,
    STDDEV_SAMP(amount) AS amount_stddev
FROM scored
GROUP BY running_avg_is_new_session_bucket_tier
ORDER BY running_avg_is_new_session_bucket_tier DESC NULLS LAST;

WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN amount >= 3200 THEN 'High' WHEN amount >= 1700 THEN 'Medium' ELSE 'Low' END AS amount_level,
        CAST(is_flagged AS INT) AS is_flagged_flag
    FROM transactions
    WHERE merchant IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        amount_level,
        is_flagged_flag,
        FIRST_VALUE(merchant, TRUE) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_merchant,
        PERCENT_RANK() OVER (PARTITION BY account_id, txn_type ORDER BY session_no DESC) AS session_no_percent_rank,
        MAX(is_new_burst) OVER (PARTITION BY account_id, merchant) AS account_id_max_is_new_burst,
        FIRST_VALUE(merchant, TRUE) OVER (PARTITION BY merchant ORDER BY session_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_merchant_2
    FROM sessions_transactions
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(amount) OVER (PARTITION BY merchant, txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_count_30d,
        CUME_DIST() OVER (PARTITION BY txn_type ORDER BY amount DESC) AS amount_cume_dist,
        MAX(account_id_max_is_new_burst) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS account_id_max_is_new_burst_max_30d,
        SUM(is_flagged_flag) OVER w_ordered AS cumulative_is_flagged_flag,
        LEAD(session_no_percent_rank, 1) OVER (PARTITION BY account_id ORDER BY session_no_percent_rank, id) AS next_session_no_percent_rank
    FROM scored
    WINDOW w_ordered AS (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored_transactions AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(next_session_no_percent_rank) OVER w_hist AS last7_min_next_session_no_percent_rank,
        AVG(next_session_no_percent_rank) OVER w_hist AS last7_avg_next_session_no_percent_rank,
        MAX(next_session_no_percent_rank) OVER w_hist AS last7_max_next_session_no_percent_rank
    FROM metrics
    WINDOW w_hist AS (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(session_no_percent_rank / amount_count_30d, 2) AS session_no_percent_rank_per_amount_count_30d
    FROM scored_transactions
    WHERE merchant IS NOT NULL
      AND merchant IS DISTINCT FROM 'Starbucks'
),
scored_transactions_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(account_id_max_is_new_burst_max_30d) OVER w_hist AS account_id_max_is_new_burst_max_30d_max_7d,
        STDDEV_SAMP(last7_max_next_session_no_percent_rank) OVER w_ordered AS txn_type_stddev_samp_last7_max_next_session_no_percent_rank,
        COUNT(account_id_max_is_new_burst) OVER (PARTITION BY account_id) AS account_id_count_account_id_max_is_new_burst
    FROM filtered
    WINDOW w_hist AS (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY txn_type)
)
-- Join a dimension and aggregate
SELECT
    dim.status,
    s.txn_type,
    COUNT(*) AS num_rows,
    ROUND(AVG(s.account_id_max_is_new_burst_max_30d_max_7d), 2) AS avg_account_id_max_is_new_burst_max_30d_max_7d,
    ROUND(AVG(s.last7_min_next_session_no_percent_rank), 2) AS avg_last7_min_next_session_no_percent_rank
FROM scored_transactions_2 s
LEFT JOIN accounts dim ON s.account_id = dim.id
GROUP BY CUBE (dim.status, s.txn_type)
ORDER BY txn_type DESC NULLS FIRST;

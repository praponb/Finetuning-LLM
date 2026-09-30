WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CONCAT_WS('-', merchant, txn_type) AS merchant_txn_type_key,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(merchant, 'UNKNOWN') AS merchant_clean
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-12-17 17:00:00'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM islands
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
matched_keys AS (
    -- Set difference on keys
    SELECT merchant, id
    FROM sessions_transactions
    EXCEPT
    SELECT merchant, id
    FROM transactions
    WHERE merchant = 'Grab'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions_transactions s
    INNER JOIN matched_keys k ON s.merchant = k.merchant AND s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(is_flagged_flag) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS is_flagged_flag_avg_90d,
        PERCENT_RANK() OVER (ORDER BY session_no) AS session_no_percent_rank
    FROM verified
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        merchant_txn_type_key,
        is_flagged_flag,
        merchant_clean,
        is_new_session,
        session_no,
        session_start_flag,
        session_id,
        is_flagged_flag_avg_90d,
        session_no_percent_rank,
        COUNT(is_flagged_flag) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS is_flagged_flag_count_7d,
        DENSE_RANK() OVER (PARTITION BY merchant ORDER BY session_no_percent_rank NULLS FIRST) AS session_no_percent_rank_dense_rank,
        SUM(session_id) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS session_id_sum_90d,
        SUM(session_id) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS session_id_sum_30d
    FROM enriched
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS span_minutes,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
    SUM(CASE WHEN merchant = 'Lazada' THEN is_new_session ELSE 0 END) AS lazada_is_new_session,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS span_minutes_2,
    SUM(CASE WHEN merchant = 'Starbucks' THEN session_id_sum_90d ELSE 0 END) AS starbucks_session_id_sum_90d,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min
FROM scored
GROUP BY 1
ORDER BY span_minutes_2 DESC NULLS LAST;

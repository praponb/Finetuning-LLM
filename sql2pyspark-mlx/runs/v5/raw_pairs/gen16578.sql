WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, value
),
unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        log_ts,
        status_code,
        key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4
    FROM exploded
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY key), 4) AS status_code_share,
        COUNT(flag_bit4) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS flag_bit4_count_2h,
        RANK() OVER (ORDER BY status_code) AS status_code_rank
    FROM unpacked
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, key) > INTERVAL 1 HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM with_windows
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY service ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status_code,
        key,
        status_code_high,
        flag_bit4,
        status_code_share,
        flag_bit4_count_2h,
        status_code_rank,
        ROW_NUMBER() OVER (PARTITION BY service, key ORDER BY status_code_high DESC, id, key) AS row_seq,
        AVG(island_id) OVER (PARTITION BY key ORDER BY status_code_share, id, key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS island_id_avg_2,
        AVG(flag_bit4) OVER (PARTITION BY key, service ORDER BY status_code_high, id, key ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS flag_bit4_avg_3_prior
    FROM sessions_app_logs
),
with_windows_app_logs AS (
    -- Running and moving aggregates
    SELECT
        id,
        status_code,
        key,
        flag_bit4,
        status_code_share,
        flag_bit4_count_2h,
        island_id_avg_2,
        flag_bit4_avg_3_prior,
        MAX(flag_bit4_avg_3_prior) OVER w_running AS last7_max_flag_bit4_avg_3_prior,
        COUNT(flag_bit4_avg_3_prior) OVER w_running AS last7_count_flag_bit4_avg_3_prior,
        AVG(flag_bit4_avg_3_prior) OVER w_running AS last7_avg_flag_bit4_avg_3_prior
    FROM scored
    WINDOW w_running AS (PARTITION BY key ORDER BY flag_bit4_avg_3_prior, id, key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    key,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list,
    COUNT(CASE WHEN key = 'trace' THEN 1 END) AS trace_count,
    COUNT(*) AS row_count,
    MIN(island_id_avg_2) AS min_island_id_avg_2,
    SIZE(COLLECT_SET(key)) AS num_keys
FROM with_windows_app_logs
GROUP BY ROLLUP (key)
HAVING COUNT(*) >= 1
ORDER BY key DESC NULLS LAST

WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, v
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key) > INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM flattened
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_burst
    FROM sessionized
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        log_ts,
        status_code,
        attr_key,
        island_id,
        session_no,
        LOWER(host) AS host_lower
    FROM sessions_app_logs
    WHERE attr_key IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key) AS row_num
    FROM filtered
    QUALIFY row_num = 1
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key) + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM ranked
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM flagged_2
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(island_id_2) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS island_id_2_count_15m,
        AVG(status_code) OVER (PARTITION BY attr_key ORDER BY island_id_2, id, attr_key ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_code_avg_6_prior,
        SUM(island_id_2) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS island_id_2_sum_15m,
        NTILE(5) OVER (ORDER BY session_start_flag, id, attr_key) AS session_start_flag_bucket,
        LAG(session_start_flag, 2) OVER (PARTITION BY attr_key, host ORDER BY session_no, id, attr_key) AS prev_session_start_flag
    FROM islands
)
-- Grouped metrics
SELECT
    island_id,
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    MAX(island_id_2_sum_15m) FILTER (WHERE host = 'h1') AS h1_max_island_id_2_sum_15m,
    COUNT(CASE WHEN host = 'h2' THEN 1 END) AS h2_count,
    GROUPING(island_id) AS grouping_id
FROM metrics
GROUP BY ROLLUP (island_id, DATE_TRUNC('HOUR', log_ts))

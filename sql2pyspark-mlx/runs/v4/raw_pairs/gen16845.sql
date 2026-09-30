WITH exploded AS (
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
    LATERAL VIEW EXPLODE(context) li AS attr_key, v
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        attr_key,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM flattened_attrs
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key) AS prev_log_ts
    FROM islands
),
flagged_2 AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session_2
    FROM with_prev
),
islands_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session_2) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged_2
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN prev_log_ts - LAG(prev_log_ts) OVER (PARTITION BY attr_key ORDER BY prev_log_ts, id, attr_key) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM islands_app_logs
),
islands_app_logs_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY attr_key ORDER BY prev_log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM gaps
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY flag_bit8 DESC, id, attr_key) AS row_num
    FROM islands_app_logs_2
    QUALIFY row_num = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', prev_log_ts) AS year_start,
    MAX(log_ts) AS last_log_ts,
    SUM(CASE WHEN service = 'search' THEN status_code_high ELSE 0 END) AS search_status_code_high,
    APPROX_PERCENTILE(status_code_high, 0.9) AS status_code_high_p90,
    PERCENTILE_APPROX(is_new_session, 0.9) AS is_new_session_p90
FROM latest
GROUP BY 1
HAVING is_new_session_p90 >= 0;

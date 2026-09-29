WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, value
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        key,
        value,
        CAST(log_ts AS DATE) AS log_ts_day,
        COALESCE(key, 'UNKNOWN') AS resolved_key
    FROM flattened
    WHERE log_ts >= DATE '2025-06-01'
),
cleaned_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        id,
        key,
        value,
        log_ts_day,
        DATE_TRUNC('WEEK', log_ts_day) AS log_ts_day_period
    FROM cleaned
    WHERE key IS NOT DISTINCT FROM 'region'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        key,
        log_ts_day_period
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY log_ts_day, id, key) AS rank_in_group
        FROM cleaned_app_logs
    ) t
    WHERE rank_in_group = 1
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts_day_period) OVER (PARTITION BY key ORDER BY log_ts_day_period, id, key) AS prev_log_ts_day_period
    FROM ranked
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts_day_period IS NULL OR log_ts_day_period > prev_log_ts_day_period + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY key ORDER BY log_ts_day_period, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Ranking and gap features
SELECT
    *,
    LEAD(new_group_flag) OVER w_part AS next_new_group_flag,
    LAG(new_group_flag) OVER w_part AS prev_new_group_flag,
    LEAD(session_no) OVER w_part AS next_session_no,
    ROW_NUMBER() OVER w_part AS seq_no
FROM islands
WINDOW w_part AS (PARTITION BY key ORDER BY prev_log_ts_day_period, id, key)

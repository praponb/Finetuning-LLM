WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        log_ts,
        level,
        status_code,
        attr_key,
        attr_value,
        CASE WHEN service IN ('billing', 'checkout') THEN 'Core' ELSE 'Other' END AS service_group
    FROM exploded
    WHERE log_ts >= DATE '2025-06-01'
      AND attr_key IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        log_ts,
        level,
        status_code,
        attr_key,
        service_group,
        CONCAT(attr_key, '/', level) AS attr_key_level,
        COALESCE(attr_key, 'DIRECT') AS resolved_attr_key
    FROM filtered
    WHERE status_code BETWEEN 261 AND 296
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        log_ts,
        level,
        attr_key,
        attr_key_level,
        resolved_attr_key,
        LAST_VALUE(attr_key) IGNORE NULLS OVER (PARTITION BY level ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_attr_key,
        STDDEV_SAMP(status_code) OVER (PARTITION BY level) AS level_stddev_samp_status_code,
        FIRST_VALUE(attr_key) IGNORE NULLS OVER (PARTITION BY level, attr_key ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_attr_key,
        ROW_NUMBER() OVER (ORDER BY status_code, id, attr_key) AS row_seq,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM cleaned
    WHERE log_ts < '2023-10-01'
    WINDOW w_grp AS (PARTITION BY attr_key ORDER BY log_ts, id, attr_key)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(row_seq) IGNORE NULLS OVER w_part AS last_known_row_seq,
        NTILE(4) OVER (ORDER BY row_seq, id, attr_key) AS row_seq_bucket
    FROM scored
    WINDOW w_part AS (PARTITION BY attr_key ORDER BY level_stddev_samp_status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, attr_key) AS prev_log_ts
    FROM with_windows
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY level ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
SELECT
    id,
    log_ts,
    level,
    attr_key,
    attr_key_level,
    resolved_attr_key,
    last_known_attr_key,
    level_stddev_samp_status_code,
    first_known_attr_key,
    CASE WHEN level RLIKE '^(WARN|ERROR)$' THEN 'OTHER_ZONE' WHEN level RLIKE '^(INFO|DEBUG)$' THEN 'EXTENDED' ELSE 'CORE' END AS level_zone
FROM islands

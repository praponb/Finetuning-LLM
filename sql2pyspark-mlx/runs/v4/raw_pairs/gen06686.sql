WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, value
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(key) IGNORE NULLS OVER w_grp first_known_key,
        LAST_VALUE(key) IGNORE NULLS OVER (PARTITION BY key, level ORDER BY log_ts ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_key
    FROM lines
    WINDOW w_grp AS (PARTITION BY key ORDER BY status_code ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts ASC, id ASC, key ASC) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts ASC, id ASC, key ASC) + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM metrics
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY level ORDER BY log_ts ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY session_start_flag ASC, id ASC, key ASC) row_seq,
        AVG(status_code) OVER (PARTITION BY level ORDER BY status_code ASC, id ASC, key ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) status_code_avg_6,
        LAST_VALUE(key, TRUE) OVER (PARTITION BY key ORDER BY session_no ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_key_2
    FROM islands
)
SELECT
    id,
    service,
    log_ts,
    key,
    first_known_key,
    last_known_key,
    status_code_avg_6
FROM enriched
ORDER BY id DESC NULLS LAST;

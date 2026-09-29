WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, v
),
cleaned AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v,
        UPPER(host) AS host_upper,
        latency_ms - status_code AS latency_ms_minus_status_code
    FROM flattened
    WHERE log_ts >= TO_TIMESTAMP('2025-06-01 21:00:00')
      AND attr_key = 'trace'
),
valid AS (
    SELECT *, CEIL(latency_ms_minus_status_code) AS latency_ms_minus_status_code_whole
    FROM cleaned
    WHERE log_ts > TIMESTAMP '2025-06-01 21:00:00'
      AND attr_key IS NOT NULL
      AND attr_key NOT IN ('region', 'version')
),
filtered AS (
    SELECT *, DAY(log_ts) AS log_ts_day
    FROM valid
    WHERE log_ts > TIMESTAMP '2025-06-01 20:00:00'
      AND attr_key IS NOT NULL
),
filtered_app_logs AS (
    SELECT
        id,
        host,
        status_code,
        attr_key,
        v,
        host_upper,
        latency_ms_minus_status_code_whole,
        log_ts_day,
        ROUND(latency_ms_minus_status_code, 0) AS latency_ms_minus_status_code_rounded
    FROM filtered
    WHERE log_ts > TO_TIMESTAMP('2025-06-02 00:00:00')
      AND attr_key IS NOT NULL
)
SELECT
    id,
    host,
    status_code,
    attr_key,
    v,
    host_upper,
    latency_ms_minus_status_code_whole,
    log_ts_day,
    latency_ms_minus_status_code_rounded,
    RANK() OVER (ORDER BY log_ts_day DESC NULLS FIRST) AS log_ts_day_rank,
    PERCENT_RANK() OVER w_running AS latency_ms_minus_status_code_rounded_percent_rank
FROM filtered_app_logs
WINDOW w_running AS (PARTITION BY attr_key ORDER BY latency_ms_minus_status_code_rounded DESC NULLS LAST)

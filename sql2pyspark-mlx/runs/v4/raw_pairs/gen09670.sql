WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, value
),
matched_keys AS (
    -- Set difference on keys
    SELECT host, id
    FROM lines
    EXCEPT
    SELECT host, id
    FROM app_logs
    WHERE service = 'checkout'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    INNER JOIN matched_keys k ON s.host = k.host AND s.id = k.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(log_ts AS DATE) AS log_ts_day
    FROM verified
    WHERE k IN ('trace', 'version')
),
filtered_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(latency_ms AS DOUBLE) AS latency_ms_num,
        DATEDIFF(CURRENT_DATE(), log_ts_day) AS days_since_log_ts_day
    FROM filtered
    WHERE log_ts > DATE '2025-06-02'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        latency_ms_num + status_code AS latency_ms_num_plus_status_code,
        HOUR(log_ts) AS log_ts_hour
    FROM filtered_app_logs
    WHERE log_ts >= TO_DATE('2025-06-01')
      AND k IS NOT NULL
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        k,
        value,
        days_since_log_ts_day,
        latency_ms_num_plus_status_code,
        ADD_MONTHS(log_ts_day, 1) AS log_ts_day_plus_months,
        CASE WHEN latency_ms_num >= 73 THEN 'High' WHEN latency_ms_num >= 29 THEN 'Medium' ELSE 'Low' END AS latency_ms_num_band
    FROM cleaned
    WHERE log_ts >= TO_DATE('2025-06-01')
      AND k IS NOT NULL
)
SELECT
    id,
    host,
    log_ts,
    latency_ms,
    k,
    value,
    days_since_log_ts_day,
    latency_ms_num_plus_status_code
FROM base
ORDER BY id DESC NULLS LAST;

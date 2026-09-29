WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, value
),
unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1
    FROM lines
    WHERE service = 'search'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(host) AS host_upper,
        CAST(status_code AS STRING) AS status_code_num
    FROM unpacked
    WHERE log_ts > TIMESTAMP '2025-06-01 12:00:00'
      AND key IS DISTINCT FROM 'trace'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(service) AS service_upper
    FROM valid
    WHERE log_ts < '2023-09-01'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        service,
        host,
        latency_ms,
        status_code,
        value,
        status_code_high,
        flag_bit1,
        host_upper,
        status_code_num,
        service_upper,
        status_code / NULLIF(status_code_high, 0) AS status_code_per_status_code_high
    FROM filtered
    WHERE key IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    service,
    CASE WHEN flag_bit1 >= 1 THEN 'HIGH' ELSE 'LOW' END AS flag_bit1_tier,
    COUNT_IF(service = 'search') AS search_events,
    COUNT(*) AS row_count,
    CORR(status_code, latency_ms) AS corr_status_code_latency_ms,
    PERCENTILE_APPROX(status_code_high, 0.5) AS status_code_high_p50,
    SIZE(COLLECT_SET(service)) AS num_services,
    SORT_ARRAY(COLLECT_SET(service)) AS service_list
FROM cleaned
GROUP BY service, CASE WHEN flag_bit1 >= 1 THEN 'HIGH' ELSE 'LOW' END
ORDER BY row_count DESC;

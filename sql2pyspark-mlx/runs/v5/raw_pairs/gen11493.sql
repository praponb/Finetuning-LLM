WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, attr_value
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        latency_ms,
        key,
        attr_value,
        INITCAP(TRIM(key)) AS key_title,
        COALESCE(key, 'N/A') AS key_filled
    FROM exploded
    WHERE TO_DATE(log_ts) < '2023-09-01'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT *, STDDEV_SAMP(latency_ms) OVER (PARTITION BY service) AS service_stddev_samp_latency_ms
    FROM base
    WHERE service = 'search'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        key,
        key_title,
        key_filled,
        service_stddev_samp_latency_ms,
        CASE WHEN key = 'trace' THEN 1 ELSE 0 END AS is_trace
    FROM enriched
    WHERE key IS DISTINCT FROM 'region'
      AND latency_ms = 983
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        key,
        COUNT(*) FILTER (WHERE service = 'auth') AS auth_rows,
        SUM(service_stddev_samp_latency_ms) FILTER (WHERE key = 'version') AS version_total_service_stddev_samp_latency_ms,
        COUNT(DISTINCT service) AS distinct_services
    FROM cleaned
    GROUP BY 1
)
SELECT
    key,
    auth_rows,
    version_total_service_stddev_samp_latency_ms,
    distinct_services,
    CASE WHEN key RLIKE '^(trace|region)$' THEN 'GROUP_A' WHEN key RLIKE '^(version)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS key_zone
FROM daily
ORDER BY key_zone;

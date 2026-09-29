WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, v
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(attr_key, '/', service) AS attr_key_service,
        UPPER(attr_key) AS attr_key_upper
    FROM flattened
    WHERE log_ts >= TO_TIMESTAMP('2025-06-01 08:00:00')
),
filtered_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        service,
        log_ts,
        latency_ms,
        status_code,
        v,
        attr_key_service,
        attr_key_upper,
        CASE WHEN service = 'search' THEN 1 ELSE 0 END AS is_search,
        TRIM(attr_key) AS clean_attr_key
    FROM filtered
    WHERE status_code < 376
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        service,
        latency_ms,
        attr_key_service,
        is_search,
        clean_attr_key,
        COUNT(status_code) OVER (PARTITION BY service) AS service_count_status_code
    FROM filtered_app_logs
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share,
        CUME_DIST() OVER (PARTITION BY service ORDER BY service_count_status_code ASC) AS service_count_status_code_cume_dist
    FROM with_windows
    WHERE service_count_status_code = 10
)
-- Grouped metrics
SELECT
    CASE WHEN latency_ms >= 2276 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_tier,
    COUNT_IF(service = 'auth') AS auth_events,
    COUNT(DISTINCT service) AS distinct_services
FROM metrics
GROUP BY latency_ms_tier
HAVING distinct_services < 1000;

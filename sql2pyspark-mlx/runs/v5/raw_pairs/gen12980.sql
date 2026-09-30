WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, attr_value
),
parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        host,
        latency_ms,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE latency_ms BETWEEN 987 AND 1742
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        latency_ms,
        is_flagged,
        status_code_high,
        ROUND(latency_ms * 1.15, 2) latency_ms_scaled,
        LOWER(service) service_lower
    FROM parsed
    WHERE latency_ms <> 610
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(is_flagged) OVER (PARTITION BY service) service_max_is_flagged,
        COUNT(is_flagged) OVER (PARTITION BY service) service_count_is_flagged,
        PERCENT_RANK() OVER (PARTITION BY service ORDER BY latency_ms ASC NULLS LAST) latency_ms_percent_rank
    FROM valid
),
valid_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(service) service_upper,
        latency_ms_percent_rank / NULLIF(service_max_is_flagged, 0) latency_ms_percent_rank_per_service_max_is_flagged
    FROM enriched
    WHERE latency_ms <= 1637
),
valid_app_logs_2 AS (
    -- Keep recent, valid rows
    SELECT
        service,
        latency_ms,
        status_code_high,
        latency_ms_scaled,
        service_lower,
        service_max_is_flagged,
        service_count_is_flagged,
        latency_ms_percent_rank_per_service_max_is_flagged,
        latency_ms_scaled * latency_ms_percent_rank_per_service_max_is_flagged latency_ms_scaled_times_latency_ms_percent_rank_per_service_max_is_flagged
    FROM valid_app_logs
    WHERE service_max_is_flagged <= 0
)
SELECT
    service,
    latency_ms,
    latency_ms_scaled,
    service_lower,
    service_max_is_flagged,
    service_count_is_flagged,
    latency_ms_percent_rank_per_service_max_is_flagged
FROM valid_app_logs_2;

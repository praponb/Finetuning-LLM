WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, attr_value
),
with_windows AS (
    -- Ranking and gap features
    SELECT *, CUME_DIST() OVER (PARTITION BY k, service ORDER BY latency_ms NULLS LAST) AS latency_ms_cume_dist
    FROM flattened
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value,
        latency_ms_cume_dist,
        latency_ms_cume_dist - status_code AS latency_ms_cume_dist_minus_status_code,
        COALESCE(k, 'NONE') AS k_clean
    FROM with_windows
    WHERE log_ts >= TIMESTAMP '2025-06-01 06:00:00'
      AND latency_ms_cume_dist < 0.29
),
enriched AS (
    -- Ranking and gap features
    SELECT *, COUNT(latency_ms) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_count_1h
    FROM cleaned
)
SELECT
    id,
    log_ts,
    latency_ms,
    status_code,
    k,
    attr_value,
    latency_ms_cume_dist,
    latency_ms_cume_dist_minus_status_code,
    k_clean,
    CASE WHEN k RLIKE '^(trace|version)$' THEN 'CORE' WHEN k RLIKE '^(region)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS k_zone
FROM enriched
ORDER BY k DESC NULLS FIRST;

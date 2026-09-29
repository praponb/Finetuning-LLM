WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, value
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(status_code) OVER w_recent AS level_stddev_samp_status_code,
        CORR(latency_ms, status_code) OVER (PARTITION BY service) AS corr_latency_ms_status_code,
        CORR(status_code, latency_ms) OVER w_recent AS corr_status_code_latency_ms
    FROM exploded
    WINDOW w_recent AS (PARTITION BY level)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        key,
        value,
        level_stddev_samp_status_code,
        corr_latency_ms_status_code,
        corr_status_code_latency_ms,
        COALESCE(key, 'unassigned') AS key_filled,
        CONCAT(key, ', ', level) AS key_level
    FROM with_windows
    WHERE latency_ms >= 1737
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(level, ' - ', key) AS level_key,
        COALESCE(key, 'DIRECT') AS key_clean
    FROM cleaned
    WHERE corr_latency_ms_status_code >= 0.0
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        level,
        latency_ms,
        status_code,
        key,
        value,
        level_stddev_samp_status_code,
        corr_latency_ms_status_code,
        corr_status_code_latency_ms,
        key_filled,
        key_level,
        level_key,
        key_clean,
        ROUND(status_code * 0.9, 2) AS status_code_adjusted,
        COALESCE(key, 'DIRECT') AS resolved_key
    FROM base
    WHERE corr_latency_ms_status_code BETWEEN 0.1 AND 0.6
),
valid_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(status_code_adjusted, 0) AS status_code_adjusted_filled,
        COALESCE(key, 'DIRECT') AS resolved_key_2
    FROM valid
    WHERE latency_ms < 979
)
-- Grouped metrics
SELECT
    CASE WHEN corr_status_code_latency_ms >= 0.59 THEN 'PREMIUM' ELSE 'STANDARD' END AS corr_status_code_latency_ms_tier,
    CASE WHEN latency_ms >= 2489 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_tier,
    COUNT(DISTINCT key) AS distinct_keys,
    COUNT(CASE WHEN level = 'DEBUG' THEN 1 END) AS debug_count,
    SUM(status_code) FILTER (WHERE key = 'region') AS region_total_status_code
FROM valid_app_logs
GROUP BY CASE WHEN corr_status_code_latency_ms >= 0.59 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN latency_ms >= 2489 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING COUNT(CASE WHEN level = 'DEBUG' THEN 1 END) > 1
ORDER BY region_total_status_code NULLS LAST

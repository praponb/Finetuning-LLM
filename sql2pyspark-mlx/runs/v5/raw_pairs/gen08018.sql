WITH exploded AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, attr_value
),
cleaned AS (
    SELECT
        *,
        INITCAP(TRIM(level)) level_title,
        UPPER(host) host_upper
    FROM exploded
    WHERE log_ts >= DATE '2025-06-01'
      AND attr_key IS NOT NULL
),
filtered AS (
    SELECT *, CASE WHEN host IN ('h1', 'h4') THEN 'Tier 1' ELSE 'Other' END host_group
    FROM cleaned
    WHERE log_ts >= DATE '2025-06-01'
      AND attr_key IS NOT NULL
),
with_windows AS (
    SELECT
        *,
        CORR(status_code, latency_ms) OVER (PARTITION BY level) corr_status_code_latency_ms,
        MAX(status_code) OVER (PARTITION BY host) host_max_status_code,
        MAX(status_code) OVER (PARTITION BY level) level_max_status_code
    FROM filtered
    WHERE status_code >= 379
),
filtered_app_logs AS (
    SELECT
        *,
        latency_ms - status_code latency_ms_minus_status_code,
        CAST(log_ts AS DATE) log_ts_day,
        COALESCE(attr_key, 'N/A') attr_key_filled
    FROM with_windows
    WHERE log_ts >= TO_TIMESTAMP('2025-06-01 15:00:00')
),
grouped AS (
    SELECT
        DATE_TRUNC('WEEK', log_ts_day) AS week_start,
        CASE WHEN corr_status_code_latency_ms >= 0.33 THEN 'HIGH' ELSE 'LOW' END AS corr_status_code_latency_ms_tier,
        level,
        COUNT_IF(level = 'DEBUG') AS debug_events,
        PERCENTILE_APPROX(status_code, ARRAY(0.25, 0.5, 0.75)) AS status_code_quartiles,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', log_ts_day) ORDER BY COUNT_IF(level = 'DEBUG') DESC) AS debug_events_dense_rank
    FROM filtered_app_logs
    GROUP BY DATE_TRUNC('WEEK', log_ts_day), CASE WHEN corr_status_code_latency_ms >= 0.33 THEN 'HIGH' ELSE 'LOW' END, level
)
SELECT
    week_start,
    corr_status_code_latency_ms_tier,
    level,
    debug_events,
    CASE WHEN level RLIKE '^(WARN|INFO)$' THEN 'GROUP_B' WHEN level RLIKE '^(ERROR|DEBUG)$' THEN 'GROUP_A' ELSE 'CORE' END AS level_zone
FROM grouped
ORDER BY week_start NULLS FIRST;

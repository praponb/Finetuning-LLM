WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, v
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(latency_ms) OVER w_running AS attr_key_sum_latency_ms,
        CORR(status_code, latency_ms) OVER w_recent AS corr_status_code_latency_ms,
        CORR(latency_ms, status_code) OVER w_recent AS corr_latency_ms_status_code,
        CUME_DIST() OVER (ORDER BY latency_ms DESC) AS latency_ms_cume_dist,
        CUME_DIST() OVER (PARTITION BY service ORDER BY latency_ms NULLS FIRST) AS latency_ms_cume_dist_2
    FROM exploded
    WHERE service IN ('checkout', 'billing')
    WINDOW w_running AS (PARTITION BY attr_key, level), w_recent AS (PARTITION BY service)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(attr_key) AS attr_key_upper,
        COALESCE(attr_key, 'OTHER') AS attr_key_filled
    FROM with_windows
    WHERE latency_ms_cume_dist <= 0.72
),
with_windows_app_logs AS (
    -- Running and moving aggregates
    SELECT
        service,
        level,
        v,
        latency_ms_cume_dist,
        CORR(latency_ms, status_code) OVER (PARTITION BY level) AS corr_latency_ms_status_code_2,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY level, service), 4) AS latency_ms_share
    FROM base
),
base_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        service,
        level,
        v,
        latency_ms_cume_dist,
        corr_latency_ms_status_code_2,
        latency_ms_share,
        ROUND(latency_ms_cume_dist, 2) AS latency_ms_cume_dist_rounded
    FROM with_windows_app_logs
    WHERE service = 'billing'
),
with_windows_app_logs_2 AS (
    -- Running and moving aggregates
    SELECT
        service,
        latency_ms_cume_dist,
        corr_latency_ms_status_code_2,
        latency_ms_share,
        latency_ms_cume_dist_rounded,
        PERCENT_RANK() OVER (ORDER BY latency_ms_cume_dist NULLS FIRST) AS latency_ms_cume_dist_percent_rank
    FROM base_app_logs
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(latency_ms_cume_dist_percent_rank AS INT) AS latency_ms_cume_dist_percent_rank_int
    FROM with_windows_app_logs_2
    WHERE latency_ms_cume_dist_rounded BETWEEN 23 AND 65
)
SELECT
    latency_ms_cume_dist,
    latency_ms_share,
    latency_ms_cume_dist_rounded,
    latency_ms_cume_dist_percent_rank,
    latency_ms_cume_dist_percent_rank_int
FROM valid
ORDER BY latency_ms_cume_dist_percent_rank;

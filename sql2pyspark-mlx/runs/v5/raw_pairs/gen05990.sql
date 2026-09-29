WITH flattened AS (
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
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY latency_ms DESC) AS latency_ms_cume_dist,
        CUME_DIST() OVER (ORDER BY status_code DESC) AS status_code_cume_dist,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY key), 4) AS status_code_share
    FROM flattened
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        key,
        value,
        latency_ms_cume_dist,
        status_code_cume_dist,
        status_code + latency_ms_cume_dist AS status_code_plus_latency_ms_cume_dist
    FROM enriched
    WHERE status_code_cume_dist < 0.34
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(service) AS service_length,
        SUBSTRING(level, 1, 4) AS level_prefix
    FROM valid
    WHERE status_code >= 352
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(latency_ms_cume_dist) OVER w_recent AS service_count_latency_ms_cume_dist,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY level), 4) AS latency_ms_share,
        COUNT(status_code_cume_dist) OVER (PARTITION BY key, service) AS key_count_status_code_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY status_code_plus_latency_ms_cume_dist) AS status_code_plus_latency_ms_cume_dist_percent_rank,
        MIN(status_code_plus_latency_ms_cume_dist) OVER w_recent_2 AS key_min_status_code_plus_latency_ms_cume_dist
    FROM filtered
    WINDOW w_recent AS (PARTITION BY service), w_recent_2 AS (PARTITION BY key)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        status_code * status_code_cume_dist AS status_code_times_status_code_cume_dist,
        COALESCE(key, 'OTHER') AS key_clean
    FROM with_windows
    WHERE service_count_latency_ms_cume_dist BETWEEN 9 AND 16
),
scored AS (
    -- Running and moving aggregates
    SELECT
        service,
        level,
        latency_ms,
        value,
        status_code_cume_dist,
        service_length,
        service_count_latency_ms_cume_dist,
        latency_ms_share,
        key_count_status_code_cume_dist,
        status_code_plus_latency_ms_cume_dist_percent_rank,
        key_min_status_code_plus_latency_ms_cume_dist,
        status_code_times_status_code_cume_dist,
        CUME_DIST() OVER (PARTITION BY level ORDER BY status_code_plus_latency_ms_cume_dist_percent_rank NULLS FIRST) AS status_code_plus_latency_ms_cume_dist_percent_rank_cume_dist,
        CORR(status_code, latency_ms) OVER (PARTITION BY level) AS corr_status_code_latency_ms,
        RANK() OVER (PARTITION BY service ORDER BY latency_ms_share DESC NULLS LAST) AS latency_ms_share_rank
    FROM cleaned
)
SELECT
    service,
    level,
    latency_ms,
    value,
    status_code_cume_dist,
    service_count_latency_ms_cume_dist,
    latency_ms_share,
    key_count_status_code_cume_dist,
    status_code_plus_latency_ms_cume_dist_percent_rank
FROM scored
ORDER BY latency_ms_share NULLS LAST, status_code_cume_dist;

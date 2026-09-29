WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, value
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        level,
        latency_ms,
        value,
        CAST(latency_ms AS DOUBLE) AS latency_ms_str,
        COALESCE(key, 'OTHER') AS key_clean
    FROM lines
    WHERE latency_ms BETWEEN 1626 AND 2044
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LEAST(latency_ms_str, latency_ms) AS least_latency_ms_str_latency_ms,
        COALESCE(latency_ms_str, 0) AS latency_ms_str_filled
    FROM base
    WHERE level IN ('ERROR', 'INFO')
),
base_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN latency_ms_str_filled >= 62 THEN 'High' WHEN latency_ms_str_filled >= 10 THEN 'Medium' ELSE 'Low' END AS latency_ms_str_filled_band,
        CASE WHEN latency_ms_str_filled >= 86 THEN 'High' WHEN latency_ms_str_filled >= 12 THEN 'Medium' ELSE 'Low' END AS latency_ms_str_filled_level
    FROM valid
    WHERE latency_ms_str_filled >= 60
),
metrics AS (
    -- Ranking and gap features
    SELECT *, ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY level), 4) AS latency_ms_share
    FROM base_app_logs
    WHERE latency_ms_str_filled > 72
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        level,
        latency_ms,
        value,
        key_clean,
        least_latency_ms_str_latency_ms,
        latency_ms_str_filled_band,
        latency_ms_share,
        PERCENT_RANK() OVER (ORDER BY latency_ms_str DESC NULLS FIRST) AS latency_ms_str_percent_rank,
        MAX(latency_ms) OVER (PARTITION BY level) AS level_max_latency_ms,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY level), 4) AS latency_ms_share_2
    FROM metrics
),
valid_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        level,
        latency_ms,
        value,
        key_clean,
        latency_ms_share,
        level_max_latency_ms,
        latency_ms_share_2 * latency_ms_share AS latency_ms_share_2_times_latency_ms_share
    FROM enriched
    WHERE level_max_latency_ms IS NOT NULL
),
stats AS (
    -- Grouped metrics
    SELECT
        level,
        BIT_AND(latency_ms) AS latency_ms_bit_and,
        APPROX_PERCENTILE(latency_ms, 0.5) AS latency_ms_p50,
        COUNT(CASE WHEN level = 'WARN' THEN 1 END) AS warn_count
    FROM valid_app_logs
    GROUP BY ROLLUP (level)
)
SELECT
    level,
    latency_ms_bit_and,
    latency_ms_p50,
    warn_count
FROM stats;

WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, attr_value
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(level)) AS level_title
    FROM flattened
    WHERE k IS NOT NULL
      AND latency_ms <> 2315
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN latency_ms >= 2632 THEN 'High' WHEN latency_ms >= 703 THEN 'Medium' ELSE 'Low' END AS latency_ms_level,
        UPPER(level) AS level_upper,
        COALESCE(k, 'NONE') AS resolved_k
    FROM valid
    WHERE host <> 'h4'
),
valid_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        id,
        host,
        level,
        latency_ms,
        k,
        attr_value,
        level_title,
        latency_ms_level,
        level_upper,
        resolved_k,
        ROUND(latency_ms * 1.15, 2) AS latency_ms_scaled,
        UPPER(host) AS host_upper,
        COALESCE(k, 'N/A') AS k_filled
    FROM filtered
    WHERE k IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(level) AS level_upper_2,
        INITCAP(TRIM(host)) AS host_title
    FROM valid_app_logs
    WHERE k IS NOT NULL
      AND latency_ms_scaled BETWEEN 28 AND 58
),
base_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(latency_ms_scaled, 0) AS latency_ms_scaled_filled,
        TRIM(level) AS clean_level,
        COALESCE(k, 'NONE') AS k_clean
    FROM base
    WHERE latency_ms_scaled > 78
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        host,
        level,
        latency_ms,
        k,
        attr_value,
        level_title,
        latency_ms_level,
        level_upper,
        resolved_k,
        latency_ms_scaled,
        host_upper,
        k_filled,
        level_upper_2,
        host_title,
        latency_ms_scaled_filled,
        clean_level,
        k_clean,
        MIN(latency_ms_scaled) OVER (PARTITION BY k, level) AS k_min_latency_ms_scaled,
        CUME_DIST() OVER (PARTITION BY host ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_cume_dist,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY latency_ms_scaled_filled DESC NULLS FIRST) AS latency_ms_scaled_filled_dense_rank
    FROM base_app_logs
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN latency_ms_scaled_filled_dense_rank >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_scaled_filled_dense_rank_tier,
        MAX(latency_ms_cume_dist) FILTER (WHERE host = 'h2') AS h2_max_latency_ms_cume_dist,
        ROUND(SUM(latency_ms) / NULLIF(COUNT(CASE WHEN k = 'version' THEN 1 END), 0), 2) AS latency_ms_per_version,
        BIT_AND(latency_ms) AS latency_ms_bit_and
    FROM with_windows
    GROUP BY CASE WHEN latency_ms_scaled_filled_dense_rank >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    latency_ms_scaled_filled_dense_rank_tier,
    h2_max_latency_ms_cume_dist,
    latency_ms_per_version,
    latency_ms_bit_and
FROM grouped
ORDER BY h2_max_latency_ms_cume_dist NULLS FIRST;

WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, attr_value
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY host ORDER BY latency_ms DESC) latency_ms_percent_rank,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY level), 4) latency_ms_share,
        PERCENT_RANK() OVER (PARTITION BY level, key ORDER BY latency_ms) latency_ms_percent_rank_2
    FROM flattened
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY key ORDER BY latency_ms_share DESC) latency_ms_share_cume_dist,
        RANK() OVER (ORDER BY latency_ms_percent_rank) latency_ms_percent_rank_rank,
        DENSE_RANK() OVER (PARTITION BY key ORDER BY latency_ms_percent_rank_2 NULLS LAST) latency_ms_percent_rank_2_dense_rank
    FROM with_windows
    WHERE key <> 'version'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(key) key_length,
        UPPER(level) level_upper,
        COALESCE(key, 'OTHER') resolved_key
    FROM scored
    WHERE latency_ms_percent_rank_2_dense_rank >= 5
),
cleaned_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN host IN ('h4', 'h3') THEN 'Core' ELSE 'Other' END host_group,
        COALESCE(key, 'N/A') resolved_key_2
    FROM cleaned
    WHERE key IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        host,
        level,
        latency_ms,
        key,
        attr_value,
        latency_ms_share_cume_dist,
        latency_ms_percent_rank_rank,
        resolved_key,
        STDDEV_SAMP(latency_ms_percent_rank_2_dense_rank) OVER (PARTITION BY host, key) host_stddev_samp_latency_ms_percent_rank_2_dense_rank,
        CUME_DIST() OVER (PARTITION BY level ORDER BY key_length DESC) key_length_cume_dist,
        DENSE_RANK() OVER (PARTITION BY key, host ORDER BY key_length DESC NULLS FIRST) key_length_dense_rank
    FROM cleaned_app_logs
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        level,
        key_length_cume_dist,
        latency_ms_share_cume_dist
    FROM metrics
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (key_length_cume_dist AS KEY_LENGTH_CUME_DIST_VAL, latency_ms_share_cume_dist)
)

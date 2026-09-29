WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, v
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(key) AS key_length,
        COALESCE(key, 'DIRECT') AS resolved_key
    FROM flattened
    WHERE level = 'ERROR'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        key,
        resolved_key,
        NTILE(4) OVER (PARTITION BY service ORDER BY latency_ms, id, key) AS latency_ms_bucket,
        AVG(key_length) OVER (ORDER BY key_length, id, key ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS key_length_avg_5_prior
    FROM cleaned
    WHERE latency_ms <> 611
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY service, key ORDER BY key_length_avg_5_prior, id, key) AS rank_in_group
    FROM scored
    QUALIFY rank_in_group = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(latency_ms_bucket) OVER (ORDER BY latency_ms_bucket, id, key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS latency_ms_bucket_avg_2,
        FIRST_VALUE(key, TRUE) OVER (ORDER BY key_length_avg_5_prior, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_key,
        SUM(key_length_avg_5_prior) OVER w_hist AS running_key_length_avg_5_prior,
        SUM(latency_ms_bucket) OVER (PARTITION BY service ORDER BY rank_in_group, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_latency_ms_bucket,
        MAX(key_length_avg_5_prior) OVER w_ordered AS key_max_key_length_avg_5_prior
    FROM latest
    WINDOW w_hist AS (PARTITION BY key ORDER BY key_length_avg_5_prior, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY key, service)
)
-- Grouped metrics
SELECT
    CASE WHEN cumulative_latency_ms_bucket >= 40 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_latency_ms_bucket_tier,
    PERCENTILE_APPROX(latency_ms_bucket, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_bucket_quartiles,
    SIZE(COLLECT_SET(key)) AS num_keys,
    COUNT(*) AS num_rows
FROM with_windows
GROUP BY CASE WHEN cumulative_latency_ms_bucket >= 40 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY cumulative_latency_ms_bucket_tier DESC;

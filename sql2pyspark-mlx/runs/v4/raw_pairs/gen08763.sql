WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, attr_value
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY k ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_rank,
        MAX(latency_ms) OVER (PARTITION BY k, service) AS k_max_latency_ms
    FROM lines
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        level,
        latency_ms,
        k,
        latency_ms_rank,
        k_max_latency_ms,
        CASE WHEN latency_ms >= 2667 THEN 'High' WHEN latency_ms >= 864 THEN 'Medium' ELSE 'Low' END AS latency_ms_level,
        COALESCE(k, 'DIRECT') AS k_filled
    FROM enriched
    WHERE level <> 'ERROR'
),
cleaned_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(service) AS clean_service,
        UPPER(k) AS k_upper,
        COALESCE(k, 'NONE') AS k_filled_2
    FROM cleaned
    WHERE k IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY level ORDER BY k_max_latency_ms DESC NULLS LAST) AS k_max_latency_ms_rank,
        RANK() OVER w_part AS k_max_latency_ms_rank_2,
        CUME_DIST() OVER (PARTITION BY service ORDER BY latency_ms_rank DESC NULLS FIRST) AS latency_ms_rank_cume_dist,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share,
        ROUND(k_max_latency_ms / SUM(k_max_latency_ms) OVER (PARTITION BY level), 4) AS k_max_latency_ms_share
    FROM cleaned_app_logs
    WINDOW w_part AS (ORDER BY k_max_latency_ms DESC NULLS LAST)
),
matched_keys AS (
    -- Set difference on keys
    SELECT service
    FROM with_windows
    EXCEPT ALL
    SELECT service
    FROM app_logs
    WHERE status_code >= 327
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN matched_keys k ON s.service = k.service
)
-- Ranking and gap features
SELECT
    *,
    CUME_DIST() OVER w_recent AS latency_ms_share_cume_dist,
    STDDEV_SAMP(latency_ms_share) OVER (PARTITION BY level, k) AS level_stddev_samp_latency_ms_share,
    PERCENT_RANK() OVER (ORDER BY latency_ms_rank DESC NULLS FIRST) AS latency_ms_rank_percent_rank,
    DENSE_RANK() OVER (PARTITION BY level ORDER BY latency_ms_share ASC NULLS LAST) AS latency_ms_share_dense_rank,
    CUME_DIST() OVER (ORDER BY latency_ms_rank_cume_dist DESC NULLS FIRST) AS latency_ms_rank_cume_dist_cume_dist
FROM verified
WINDOW w_recent AS (ORDER BY latency_ms_share ASC NULLS LAST);

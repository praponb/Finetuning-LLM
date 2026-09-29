WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, attr_value
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(latency_ms) OVER (PARTITION BY service) service_avg_latency_ms,
        RANK() OVER (ORDER BY latency_ms DESC) latency_ms_rank,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) latency_ms_share
    FROM exploded
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        latency_ms,
        attr_value,
        service_avg_latency_ms,
        latency_ms_rank,
        latency_ms_share,
        CAST(latency_ms_share AS INT) latency_ms_share_int,
        CASE WHEN attr_key = 'version' THEN 1 ELSE 0 END is_version
    FROM scored
    WHERE attr_key IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(latency_ms_share) OVER w_recent service_sum_latency_ms_share,
        RANK() OVER w_part latency_ms_rank_2,
        DENSE_RANK() OVER w_hist latency_ms_dense_rank,
        RANK() OVER (PARTITION BY service ORDER BY latency_ms_share_int NULLS FIRST) latency_ms_share_int_rank,
        MIN(latency_ms_share) OVER w_recent service_min_latency_ms_share
    FROM base
    WINDOW w_recent AS (PARTITION BY service), w_part AS (PARTITION BY service ORDER BY latency_ms), w_hist AS (PARTITION BY service ORDER BY latency_ms DESC)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        latency_ms,
        attr_value,
        service_avg_latency_ms,
        latency_ms_rank,
        latency_ms_share,
        is_version,
        service_sum_latency_ms_share,
        latency_ms_rank_2,
        service_min_latency_ms_share,
        COALESCE(latency_ms_rank_2, 0) latency_ms_rank_2_filled,
        latency_ms_share * service_min_latency_ms_share latency_ms_share_times_service_min_latency_ms_share
    FROM metrics
    WHERE latency_ms_dense_rank <= 3
)
-- Grouped metrics
SELECT
    CASE WHEN is_version >= 71 THEN 'LARGE' ELSE 'SMALL' END AS is_version_tier,
    CASE WHEN latency_ms_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_rank_tier,
    ROUND(AVG(latency_ms_share_times_service_min_latency_ms_share), 2) AS avg_latency_ms_share_times_service_min_latency_ms_share,
    PERCENTILE_APPROX(service_sum_latency_ms_share, ARRAY(0.25, 0.5, 0.75)) AS service_sum_latency_ms_share_quartiles,
    ROUND(AVG(service_min_latency_ms_share), 2) AS avg_service_min_latency_ms_share,
    BIT_OR(latency_ms) AS latency_ms_bit_or,
    BIT_AND(latency_ms) AS latency_ms_bit_and
FROM valid
GROUP BY is_version_tier, latency_ms_rank_tier
HAVING avg_service_min_latency_ms_share < 0
ORDER BY latency_ms_bit_or DESC NULLS LAST

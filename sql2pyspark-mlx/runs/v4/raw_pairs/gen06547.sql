WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, v
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        latency_ms,
        key,
        v,
        DENSE_RANK() OVER (PARTITION BY key, service ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_dense_rank,
        RANK() OVER (ORDER BY latency_ms ASC NULLS LAST) AS latency_ms_rank,
        PERCENT_RANK() OVER (ORDER BY latency_ms DESC) AS latency_ms_percent_rank
    FROM flattened
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        latency_ms,
        key,
        v,
        latency_ms_dense_rank,
        latency_ms_rank,
        latency_ms_percent_rank,
        RANK() OVER (ORDER BY latency_ms DESC) AS latency_ms_rank_2,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service, key), 4) AS latency_ms_share,
        MAX(latency_ms_rank) OVER (PARTITION BY key) AS key_max_latency_ms_rank,
        RANK() OVER (PARTITION BY service, key ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_rank_3
    FROM metrics
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(latency_ms_rank_3 / SUM(latency_ms_rank_3) OVER (PARTITION BY service), 4) AS latency_ms_rank_3_share,
        RANK() OVER (PARTITION BY key, service ORDER BY latency_ms_dense_rank ASC NULLS FIRST) AS latency_ms_dense_rank_rank
    FROM enriched
),
scored_app_logs AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        latency_ms,
        key,
        v,
        latency_ms_dense_rank,
        latency_ms_rank,
        latency_ms_percent_rank,
        latency_ms_rank_2,
        latency_ms_share,
        key_max_latency_ms_rank,
        latency_ms_rank_3,
        latency_ms_rank_3_share,
        latency_ms_dense_rank_rank,
        MIN(latency_ms_rank_2) OVER (PARTITION BY service) AS service_min_latency_ms_rank_2,
        CUME_DIST() OVER (ORDER BY latency_ms_dense_rank_rank DESC NULLS LAST) AS latency_ms_dense_rank_rank_cume_dist
    FROM scored
),
metrics_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share_2,
        AVG(latency_ms_dense_rank) OVER (PARTITION BY service, key) AS service_avg_latency_ms_dense_rank,
        PERCENT_RANK() OVER (ORDER BY latency_ms_dense_rank_rank DESC) AS latency_ms_dense_rank_rank_percent_rank,
        ROUND(latency_ms_dense_rank_rank / SUM(latency_ms_dense_rank_rank) OVER (PARTITION BY service), 4) AS latency_ms_dense_rank_rank_share,
        RANK() OVER (ORDER BY latency_ms_rank_3 ASC) AS latency_ms_rank_3_rank
    FROM scored_app_logs
)
-- Grouped metrics
SELECT
    service,
    PERCENTILE_APPROX(latency_ms_rank_3_rank, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_rank_3_rank_quartiles,
    SUM(CASE WHEN key = 'region' THEN latency_ms_rank_3_share ELSE 0 END) AS region_latency_ms_rank_3_share,
    MIN(latency_ms) AS min_latency_ms
FROM metrics_app_logs
GROUP BY service

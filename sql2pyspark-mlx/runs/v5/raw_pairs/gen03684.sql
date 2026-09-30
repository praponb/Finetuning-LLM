WITH flattened AS (
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, v
),
enriched AS (
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY service), 4) AS status_code_share,
        RANK() OVER (PARTITION BY key ORDER BY latency_ms ASC NULLS FIRST) AS latency_ms_rank,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service, key), 4) AS latency_ms_share,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share_2
    FROM flattened
),
metrics AS (
    SELECT
        id,
        status_code,
        v,
        latency_ms_rank,
        latency_ms_share,
        ROUND(latency_ms_rank / SUM(latency_ms_rank) OVER (PARTITION BY key), 4) AS latency_ms_rank_share,
        RANK() OVER (ORDER BY latency_ms_share ASC) AS latency_ms_share_rank,
        DENSE_RANK() OVER (ORDER BY status_code ASC) AS status_code_dense_rank
    FROM enriched
),
valid AS (
    SELECT
        status_code,
        v,
        latency_ms_rank,
        latency_ms_share,
        latency_ms_rank_share,
        latency_ms_share_rank,
        GREATEST(latency_ms_share, latency_ms_rank) AS greatest_latency_ms_share_latency_ms_rank
    FROM metrics
    WHERE latency_ms_share <= 0.69
),
filtered AS (
    SELECT *, GREATEST(latency_ms_rank_share, latency_ms_rank) AS greatest_latency_ms_rank_share_latency_ms_rank
    FROM valid
    WHERE greatest_latency_ms_share_latency_ms_rank >= 34
),
filtered_app_logs AS (
    SELECT *, GREATEST(latency_ms_share_rank, greatest_latency_ms_rank_share_latency_ms_rank) AS greatest_latency_ms_share_rank_greatest_latency_ms_rank_share_latency_ms_rank
    FROM filtered
    WHERE latency_ms_share <= 0.41
)
SELECT
    CASE WHEN status_code >= 394 THEN 'LARGE' ELSE 'SMALL' END AS status_code_tier,
    CASE WHEN greatest_latency_ms_rank_share_latency_ms_rank >= 71 THEN 'HIGH' ELSE 'LOW' END AS greatest_latency_ms_rank_share_latency_ms_rank_tier,
    SUM(latency_ms_share) AS total_latency_ms_share,
    PERCENTILE_APPROX(latency_ms_share_rank, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_share_rank_quartiles,
    COUNT(*) AS num_rows
FROM filtered_app_logs
GROUP BY status_code_tier, greatest_latency_ms_rank_share_latency_ms_rank_tier
ORDER BY num_rows DESC;

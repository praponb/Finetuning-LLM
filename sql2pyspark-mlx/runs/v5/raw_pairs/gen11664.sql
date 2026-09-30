WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, value
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        DENSE_RANK() OVER (ORDER BY status_code DESC) AS status_code_dense_rank,
        RANK() OVER (PARTITION BY k ORDER BY status_code DESC) AS status_code_rank,
        RANK() OVER (ORDER BY latency_ms DESC) AS latency_ms_rank,
        CUME_DIST() OVER (ORDER BY status_code) AS status_code_cume_dist,
        CORR(latency_ms, status_code) OVER (PARTITION BY service) AS corr_latency_ms_status_code
    FROM flattened
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        status_code_dense_rank,
        latency_ms_rank,
        status_code_cume_dist,
        COALESCE(latency_ms_rank, 0) AS latency_ms_rank_filled,
        corr_latency_ms_status_code + status_code_cume_dist AS corr_latency_ms_status_code_plus_status_code_cume_dist
    FROM metrics
    WHERE latency_ms_rank BETWEEN 3 AND 8
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        host,
        status_code_cume_dist,
        DENSE_RANK() OVER w_running AS status_code_cume_dist_dense_rank,
        STDDEV_SAMP(status_code_cume_dist) OVER (PARTITION BY host) AS host_stddev_samp_status_code_cume_dist
    FROM base
    WINDOW w_running AS (PARTITION BY host ORDER BY status_code_cume_dist NULLS LAST)
),
base_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        id,
        host,
        status_code_cume_dist_dense_rank,
        CASE WHEN status_code_cume_dist_dense_rank >= 8 THEN 'High' WHEN status_code_cume_dist_dense_rank >= 4 THEN 'Medium' ELSE 'Low' END AS status_code_cume_dist_dense_rank_level
    FROM enriched
    WHERE status_code_cume_dist_dense_rank BETWEEN 6 AND 7
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(host)) AS host_title
    FROM base_app_logs
    WHERE status_code_cume_dist_dense_rank > 7
)
-- Ranking and gap features
SELECT
    *,
    ROUND(status_code_cume_dist_dense_rank / SUM(status_code_cume_dist_dense_rank) OVER (PARTITION BY host), 4) AS status_code_cume_dist_dense_rank_share,
    DENSE_RANK() OVER (ORDER BY status_code_cume_dist_dense_rank DESC) AS status_code_cume_dist_dense_rank_dense_rank,
    PERCENT_RANK() OVER (PARTITION BY host ORDER BY status_code_cume_dist_dense_rank) AS status_code_cume_dist_dense_rank_percent_rank,
    CUME_DIST() OVER (PARTITION BY host ORDER BY status_code_cume_dist_dense_rank DESC NULLS LAST) AS status_code_cume_dist_dense_rank_cume_dist
FROM filtered;

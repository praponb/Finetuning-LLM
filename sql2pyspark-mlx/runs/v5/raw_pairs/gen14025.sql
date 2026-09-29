WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(host, 1, 4) AS host_prefix,
        UPPER(host) AS host_upper
    FROM lines
    WHERE k IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_dense_rank,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY host, service), 4) AS status_code_share,
        SUM(latency_ms) OVER (PARTITION BY service) AS service_sum_latency_ms,
        CORR(status_code, latency_ms) OVER (PARTITION BY service) AS corr_status_code_latency_ms,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY service), 4) AS status_code_share_2
    FROM valid
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        service,
        host,
        status_code,
        k,
        host_prefix,
        host_upper,
        status_code_share,
        corr_status_code_latency_ms,
        status_code_share_2,
        DENSE_RANK() OVER (ORDER BY status_code_share_2) AS status_code_share_2_dense_rank,
        ROUND(latency_ms_dense_rank / SUM(latency_ms_dense_rank) OVER (PARTITION BY level), 4) AS latency_ms_dense_rank_share,
        RANK() OVER (ORDER BY corr_status_code_latency_ms DESC) AS corr_status_code_latency_ms_rank,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY status_code_share NULLS FIRST) AS status_code_share_dense_rank,
        CUME_DIST() OVER (ORDER BY status_code_share_2 DESC) AS status_code_share_2_cume_dist
    FROM metrics
),
enriched_app_logs AS (
    -- Running and moving aggregates
    SELECT
        service,
        k,
        host_prefix,
        status_code_share,
        corr_status_code_latency_ms,
        status_code_share_2,
        status_code_share_2_dense_rank,
        corr_status_code_latency_ms_rank,
        status_code_share_dense_rank,
        CUME_DIST() OVER (PARTITION BY host ORDER BY status_code_share DESC) AS status_code_share_cume_dist,
        RANK() OVER (PARTITION BY k ORDER BY status_code_share_2_dense_rank NULLS FIRST) AS status_code_share_2_dense_rank_rank,
        STDDEV_SAMP(latency_ms_dense_rank_share) OVER (PARTITION BY k, service) AS k_stddev_samp_latency_ms_dense_rank_share,
        RANK() OVER (PARTITION BY host ORDER BY status_code_share NULLS FIRST) AS status_code_share_rank
    FROM enriched
    WHERE service = 'search'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        corr_status_code_latency_ms + status_code_share_2_dense_rank_rank AS corr_status_code_latency_ms_plus_status_code_share_2_dense_rank_rank,
        LEAST(corr_status_code_latency_ms, status_code_share_cume_dist) AS least_corr_status_code_latency_ms_status_code_share_cume_dist,
        COALESCE(k, 'UNKNOWN') AS k_clean
    FROM enriched_app_logs
    WHERE k IS DISTINCT FROM 'region'
      AND k_stddev_samp_latency_ms_dense_rank_share < 0
),
valid_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        service,
        host_prefix,
        status_code_share,
        corr_status_code_latency_ms,
        status_code_share_2,
        status_code_share_cume_dist,
        status_code_share_2_dense_rank_rank,
        k_stddev_samp_latency_ms_dense_rank_share,
        corr_status_code_latency_ms_plus_status_code_share_2_dense_rank_rank,
        least_corr_status_code_latency_ms_status_code_share_cume_dist,
        k_clean,
        SUBSTRING(service, 1, 4) AS service_prefix
    FROM base
    WHERE k IS NOT NULL
      AND status_code_share_dense_rank <> 3
)
-- Multi-level aggregation
SELECT
    service,
    ROUND(SUM(least_corr_status_code_latency_ms_status_code_share_cume_dist) / NULLIF(COUNT(CASE WHEN service = 'checkout' THEN 1 END), 0), 2) AS least_corr_status_code_latency_ms_status_code_share_cume_dist_per_checkout,
    COUNT(CASE WHEN service = 'checkout' THEN 1 END) AS checkout_count,
    AVG(status_code_share) FILTER (WHERE service = 'billing') AS billing_avg_status_code_share,
    PERCENTILE_APPROX(status_code_share, 0.95) AS status_code_share_p95,
    ROUND(SUM(status_code_share) / NULLIF(COUNT(CASE WHEN service = 'search' THEN 1 END), 0), 2) AS status_code_share_per_search
FROM valid_app_logs
GROUP BY 1
HAVING billing_avg_status_code_share >= 0
ORDER BY least_corr_status_code_latency_ms_status_code_share_cume_dist_per_checkout DESC;

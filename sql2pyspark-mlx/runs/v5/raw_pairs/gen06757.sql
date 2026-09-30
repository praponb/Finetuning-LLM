WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, v
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        service,
        host,
        status_code,
        v,
        MIN(status_code) OVER (PARTITION BY host, level) AS host_min_status_code,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY level), 4) AS status_code_share,
        RANK() OVER (PARTITION BY level ORDER BY status_code DESC) AS status_code_rank
    FROM lines
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER w_grp AS host_min_status_code_rank,
        STDDEV_SAMP(host_min_status_code) OVER (PARTITION BY service) AS service_stddev_samp_host_min_status_code,
        RANK() OVER w_ordered AS status_code_rank_2,
        RANK() OVER w_ordered AS status_code_rank_3
    FROM with_windows
    WHERE status_code_rank >= 7
    WINDOW w_grp AS (PARTITION BY service, host ORDER BY host_min_status_code), w_ordered AS (PARTITION BY service ORDER BY status_code)
),
enriched_app_logs AS (
    -- Window metrics over several different partitions
    SELECT *, COUNT(status_code) OVER (PARTITION BY host) AS host_count_status_code
    FROM enriched
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN service = 'search' THEN 1 ELSE 0 END AS is_search
    FROM enriched_app_logs
    WHERE host_count_status_code <= 15
)
-- Multi-level aggregation
SELECT
    CASE WHEN host_min_status_code_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS host_min_status_code_rank_tier,
    service,
    PERCENTILE_APPROX(status_code_share, 0.9) AS status_code_share_p90,
    ROUND(AVG(service_stddev_samp_host_min_status_code), 2) AS avg_service_stddev_samp_host_min_status_code,
    COUNT(DISTINCT service) AS distinct_services,
    ROUND(AVG(service_stddev_samp_host_min_status_code), 2) AS avg_service_stddev_samp_host_min_status_code_2,
    APPROX_PERCENTILE(status_code_share, 0.9) AS status_code_share_p90_2,
    ROUND(SUM(service_stddev_samp_host_min_status_code) / NULLIF(COUNT(CASE WHEN service = 'billing' THEN 1 END), 0), 2) AS service_stddev_samp_host_min_status_code_per_billing,
    RANK() OVER (PARTITION BY CASE WHEN host_min_status_code_rank >= 8 THEN 'HIGH' ELSE 'LOW' END ORDER BY PERCENTILE_APPROX(status_code_share, 0.9) DESC) AS status_code_share_p90_rank
FROM base
GROUP BY CASE WHEN host_min_status_code_rank >= 8 THEN 'HIGH' ELSE 'LOW' END, service;

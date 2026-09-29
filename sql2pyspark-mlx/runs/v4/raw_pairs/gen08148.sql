WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, v
),
with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY status_code) AS status_code_cume_dist,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY service), 4) AS status_code_share
    FROM flattened
),
with_windows_app_logs AS (
    SELECT
        *,
        MIN(status_code_cume_dist) OVER (PARTITION BY service) AS service_min_status_code_cume_dist,
        RANK() OVER w_recent AS status_code_share_rank,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) AS status_code_share_2
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY attr_key ORDER BY status_code_share DESC NULLS LAST)
)
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    PERCENTILE_APPROX(status_code_share_rank, ARRAY(0.25, 0.5, 0.75)) AS status_code_share_rank_quartiles,
    ROUND(AVG(status_code_cume_dist), 2) AS avg_status_code_cume_dist,
    SIZE(COLLECT_SET(service)) AS num_services
FROM with_windows_app_logs
GROUP BY DATE_TRUNC('DAY', log_ts)

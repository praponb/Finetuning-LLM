WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, value
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        k,
        value,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY host), 4) AS latency_ms_share,
        PERCENT_RANK() OVER w_running AS status_code_percent_rank,
        CUME_DIST() OVER (ORDER BY status_code) AS status_code_cume_dist
    FROM lines
    WINDOW w_running AS (PARTITION BY host ORDER BY status_code DESC)
),
with_windows_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY status_code) AS status_code_rank,
        CORR(latency_ms, status_code) OVER (PARTITION BY k) AS corr_latency_ms_status_code
    FROM with_windows
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY host ORDER BY status_code DESC) AS status_code_cume_dist_2,
        SUM(status_code_cume_dist) OVER (PARTITION BY host, k) AS host_sum_status_code_cume_dist,
        DENSE_RANK() OVER (ORDER BY status_code DESC) AS status_code_dense_rank,
        PERCENT_RANK() OVER (ORDER BY status_code_cume_dist NULLS LAST) AS status_code_cume_dist_percent_rank,
        ROUND(status_code_rank / SUM(status_code_rank) OVER (PARTITION BY host, k), 4) AS status_code_rank_share
    FROM with_windows_app_logs
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        k,
        value,
        latency_ms_share,
        status_code_percent_rank,
        status_code_cume_dist,
        status_code_rank,
        corr_latency_ms_status_code,
        status_code_cume_dist_2,
        host_sum_status_code_cume_dist,
        status_code_dense_rank,
        status_code_cume_dist_percent_rank,
        status_code_rank_share,
        CUME_DIST() OVER (PARTITION BY k, host ORDER BY status_code_cume_dist DESC NULLS LAST) AS status_code_cume_dist_cume_dist,
        MAX(status_code_percent_rank) OVER (PARTITION BY host) AS host_max_status_code_percent_rank
    FROM scored
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        host,
        k,
        status_code_rank_share
    FROM enriched
) src
PIVOT (
    SUM(status_code_rank_share) AS amount,
    COUNT(1) AS n
    FOR k IN ('region', 'version', 'trace')
);

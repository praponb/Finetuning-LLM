WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, value
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(latency_ms) OVER (PARTITION BY host) AS host_max_latency_ms,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY host), 4) AS latency_ms_share,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY host), 4) AS latency_ms_share_2,
        COUNT(latency_ms) OVER (PARTITION BY k) AS k_count_latency_ms,
        CUME_DIST() OVER (PARTITION BY k, host ORDER BY latency_ms DESC) AS latency_ms_cume_dist
    FROM lines
),
metrics_app_logs AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER w_ordered AS host_max_latency_ms_rank,
        RANK() OVER w_ordered AS host_max_latency_ms_rank_2,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY host), 4) AS latency_ms_share_3
    FROM metrics
    WINDOW w_ordered AS (ORDER BY host_max_latency_ms DESC NULLS FIRST)
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN k_count_latency_ms >= 18 THEN 'PREMIUM' ELSE 'STANDARD' END AS k_count_latency_ms_tier,
        host,
        k,
        PERCENTILE_APPROX(host_max_latency_ms_rank_2, 0.9) AS host_max_latency_ms_rank_2_p90,
        SUM(CASE WHEN host = 'h2' THEN host_max_latency_ms ELSE 0 END) AS h2_host_max_latency_ms,
        COUNT(DISTINCT host) AS distinct_hosts,
        PERCENTILE_APPROX(host_max_latency_ms_rank_2, 0.5) AS host_max_latency_ms_rank_2_p50,
        DENSE_RANK() OVER (PARTITION BY CASE WHEN k_count_latency_ms >= 18 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(DISTINCT host) DESC) AS distinct_hosts_dense_rank
    FROM metrics_app_logs
    GROUP BY CASE WHEN k_count_latency_ms >= 18 THEN 'PREMIUM' ELSE 'STANDARD' END, host, k
    HAVING host_max_latency_ms_rank_2_p90 > 0
)
SELECT
    k_count_latency_ms_tier,
    host,
    k,
    host_max_latency_ms_rank_2_p90,
    distinct_hosts,
    distinct_hosts_dense_rank
FROM summary
ORDER BY k_count_latency_ms_tier DESC NULLS LAST;

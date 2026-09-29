WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, v
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        k,
        MAX(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS latency_ms_max_2h
    FROM exploded
    WHERE latency_ms BETWEEN 1101 AND 2245
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        k,
        latency_ms_max_2h,
        CASE WHEN k IN ('region', 'trace') THEN 'Priority' ELSE 'Other' END AS k_group
    FROM scored
    WHERE k IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        service,
        log_ts,
        latency_ms,
        latency_ms_max_2h,
        k_group,
        MIN(latency_ms) OVER (PARTITION BY k) AS k_min_latency_ms,
        PERCENT_RANK() OVER (PARTITION BY k ORDER BY latency_ms_max_2h DESC) AS latency_ms_max_2h_percent_rank
    FROM base
),
scored_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY service ORDER BY k_min_latency_ms NULLS FIRST) AS k_min_latency_ms_rank,
        SUM(latency_ms) OVER w_recent AS latency_ms_sum_30m
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
),
scored_app_logs_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY service ORDER BY latency_ms_sum_30m DESC) AS latency_ms_sum_30m_percent_rank,
        COUNT(latency_ms_max_2h) OVER w_running AS latency_ms_max_2h_count_15m,
        MAX(latency_ms) OVER (PARTITION BY service) AS service_max_latency_ms,
        AVG(k_min_latency_ms_rank) OVER w_recent AS k_min_latency_ms_rank_avg_1h,
        ROUND(latency_ms_max_2h / SUM(latency_ms_max_2h) OVER (PARTITION BY service), 4) AS latency_ms_max_2h_share
    FROM scored_app_logs
    WINDOW w_running AS (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN service_max_latency_ms >= 2456 THEN 'LARGE' ELSE 'SMALL' END AS service_max_latency_ms_tier,
        DATE_TRUNC('DAY', log_ts) AS activity_day,
        SUM(CASE WHEN service = 'billing' THEN latency_ms_sum_30m_percent_rank ELSE 0 END) AS billing_latency_ms_sum_30m_percent_rank,
        AVG(latency_ms_max_2h_percent_rank) FILTER (WHERE service = 'search') AS search_avg_latency_ms_max_2h_percent_rank,
        SORT_ARRAY(COLLECT_SET(service)) AS service_list,
        SIZE(COLLECT_SET(service)) AS num_services
    FROM scored_app_logs_2
    GROUP BY service_max_latency_ms_tier, activity_day
)
SELECT
    service_max_latency_ms_tier,
    activity_day,
    billing_latency_ms_sum_30m_percent_rank,
    search_avg_latency_ms_max_2h_percent_rank,
    service_list,
    num_services
FROM summary
ORDER BY service_max_latency_ms_tier DESC, activity_day

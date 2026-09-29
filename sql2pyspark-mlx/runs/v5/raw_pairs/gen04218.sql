WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, v
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        status_code,
        attr_key,
        v,
        SUM(latency_ms) OVER (PARTITION BY host) AS host_sum_latency_ms,
        MAX(latency_ms) OVER w_running AS latency_ms_max_1h,
        MAX(latency_ms) OVER (PARTITION BY service, host) AS service_max_latency_ms
    FROM exploded
    WINDOW w_running AS (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY attr_key ORDER BY latency_ms_max_1h DESC NULLS LAST) AS latency_ms_max_1h_rank,
        RANK() OVER w_hist AS service_max_latency_ms_rank
    FROM scored
    WINDOW w_hist AS (PARTITION BY attr_key ORDER BY service_max_latency_ms DESC NULLS LAST)
),
scored_app_logs AS (
    -- Running and moving aggregates
    SELECT *, AVG(host_sum_latency_ms) OVER w_grp AS attr_key_avg_host_sum_latency_ms
    FROM enriched
    WHERE attr_key = 'region'
    WINDOW w_grp AS (PARTITION BY attr_key)
)
SELECT
    id,
    status_code,
    attr_key,
    service_max_latency_ms,
    service_max_latency_ms_rank,
    attr_key_avg_host_sum_latency_ms,
    CASE WHEN attr_key RLIKE '^(region|trace)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS attr_key_zone
FROM scored_app_logs
ORDER BY status_code DESC NULLS FIRST, attr_key_zone;

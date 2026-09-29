WITH flattened_attrs AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        payload_json,
        COALESCE(context['trace'], 'none') AS trace_or_default,
        context['trace'] AS trace,
        SIZE(context) AS num_context,
        ARRAY_CONTAINS(MAP_KEYS(context), 'trace') AS has_trace
    FROM app_logs
    WHERE service = 'checkout'
)
SELECT
    id,
    latency_ms,
    status_code,
    has_trace,
    SUM(latency_ms) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_sum_1h,
    PERCENT_RANK() OVER w_grp AS latency_ms_percent_rank,
    LAST_VALUE(latency_ms, TRUE) OVER (PARTITION BY service ORDER BY status_code, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_latency_ms
FROM flattened_attrs
WINDOW w_grp AS (PARTITION BY service ORDER BY latency_ms);

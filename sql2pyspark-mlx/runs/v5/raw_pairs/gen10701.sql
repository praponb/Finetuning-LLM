WITH flattened_attrs AS (
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        context,
        COALESCE(context['trace'], 'none') AS trace_or_default,
        COALESCE(context['region'], 'unknown') AS region_or_default,
        ARRAY_CONTAINS(MAP_KEYS(context), 'region') AS has_region,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        SIZE(context) AS num_context
    FROM app_logs
)
SELECT
    id,
    host,
    latency_ms,
    has_region,
    duration_text,
    ROW_NUMBER() OVER (PARTITION BY host ORDER BY num_context DESC, id) AS event_seq,
    SUM(num_context) OVER (PARTITION BY host ORDER BY log_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_num_context
FROM flattened_attrs
WHERE HOUR(log_ts) BETWEEN 7 AND 15;

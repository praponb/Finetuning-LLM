WITH parsed AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        context,
        COALESCE(context['region'], 'direct') AS region_or_default,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        SIZE(context) AS num_context
    FROM app_logs
)
SELECT
    *,
    STDDEV_SAMP(latency_ms) OVER w_ordered AS service_stddev_samp_latency_ms,
    MAX(num_context) OVER w_running AS num_context_max_30m,
    AVG(num_context) OVER w_hist AS level_avg_num_context
FROM parsed
WINDOW w_ordered AS (PARTITION BY service), w_running AS (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY level)

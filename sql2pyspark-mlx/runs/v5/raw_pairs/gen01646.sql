WITH parsed AS (
    SELECT
        id,
        service,
        host,
        latency_ms,
        payload_json,
        SIZE(context) AS num_context,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id
    FROM app_logs
)
SELECT
    id,
    service,
    latency_ms,
    num_context,
    user_id,
    SUM(num_context) OVER (PARTITION BY host) AS host_sum_num_context,
    AVG(latency_ms) OVER (PARTITION BY host ORDER BY num_context, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS latency_ms_avg_6_prior,
    LAST_VALUE(num_context, TRUE) OVER (PARTITION BY service ORDER BY latency_ms, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_num_context
FROM parsed;

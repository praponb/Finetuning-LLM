WITH flattened_attrs AS (
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        payload_json,
        context,
        context['trace'] AS trace,
        SIZE(context) AS num_context,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id
    FROM app_logs
)
SELECT
    CASE WHEN num_context >= 60 THEN 'HIGH' ELSE 'LOW' END AS num_context_tier,
    CASE WHEN latency_ms >= 2505 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_tier,
    PERCENTILE_APPROX(status_code, 0.5) AS status_code_p50,
    SUM(num_context) AS total_num_context,
    ROUND(AVG(num_context), 2) AS avg_num_context
FROM flattened_attrs
GROUP BY num_context_tier, latency_ms_tier
ORDER BY status_code_p50 DESC NULLS LAST

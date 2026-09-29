WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        payload_json,
        context,
        SIZE(context) AS num_context,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id
    FROM app_logs
)
-- Ranking and gap features
SELECT
    id,
    log_ts,
    latency_ms,
    status_code,
    user_id,
    COUNT(status_code) OVER w_grp AS running_count_status_code,
    AVG(status_code) OVER w_grp AS running_avg_status_code,
    MAX(status_code) OVER w_grp AS running_max_status_code,
    LAST_VALUE(latency_ms, TRUE) OVER w_ordered AS last_known_latency_ms
FROM flattened_attrs
WHERE level != 'WARN'
WINDOW w_grp AS (PARTITION BY level ORDER BY status_code, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY level ORDER BY num_context, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

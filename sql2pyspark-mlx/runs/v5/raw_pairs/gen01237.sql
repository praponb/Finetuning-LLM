WITH parsed AS (
    SELECT
        id,
        service,
        level,
        context,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        COALESCE(context['version'], 'unknown') AS version_or_default,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        COALESCE(context['region'], 'none') AS region_or_default
    FROM app_logs
    WHERE service <> 'checkout'
)
SELECT
    id,
    user_id,
    version_or_default,
    AVG(status_code_high) OVER (PARTITION BY service ORDER BY status_code_high, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS status_code_high_avg_5_prior,
    LEAD(status_code_high, 1) OVER (PARTITION BY level ORDER BY status_code_high, id) AS next_status_code_high,
    CUME_DIST() OVER (PARTITION BY level ORDER BY status_code_high DESC NULLS FIRST) AS status_code_high_cume_dist
FROM parsed
WHERE status_code_high >= 1

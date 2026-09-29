WITH parsed AS (
    SELECT
        id,
        service,
        SIZE(context) AS num_context,
        context['version'] AS version,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier
    FROM app_logs
)
SELECT
    CASE WHEN flag_bit2 >= 1 THEN 'LARGE' ELSE 'SMALL' END AS flag_bit2_tier,
    service,
    ROUND(SUM(num_context) / NULLIF(COUNT(CASE WHEN service = 'auth' THEN 1 END), 0), 2) AS num_context_per_auth,
    SUM(CASE WHEN service = 'auth' THEN num_context ELSE 0 END) AS auth_num_context,
    MIN(flag_bit2) AS min_flag_bit2
FROM parsed
GROUP BY GROUPING SETS ((CASE WHEN flag_bit2 >= 1 THEN 'LARGE' ELSE 'SMALL' END, service), (CASE WHEN flag_bit2 >= 1 THEN 'LARGE' ELSE 'SMALL' END))
ORDER BY service;

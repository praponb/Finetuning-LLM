WITH parsed AS (
    SELECT
        id,
        service,
        level,
        context,
        ARRAY_CONTAINS(MAP_KEYS(context), 'version') AS has_version,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2
    FROM app_logs
)
SELECT
    service,
    CASE WHEN flag_bit2 >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS flag_bit2_tier,
    level,
    APPROX_PERCENTILE(flag_bit2, 0.95) AS flag_bit2_p95,
    SUM(flag_bit2) FILTER (WHERE service = 'billing') AS billing_total_flag_bit2,
    COUNT(CASE WHEN service = 'search' THEN 1 END) AS search_count,
    COUNT_IF(service = 'billing') AS billing_events,
    MAX(flag_bit2) FILTER (WHERE level = 'INFO') AS info_max_flag_bit2,
    PERCENT_RANK() OVER (PARTITION BY service ORDER BY COUNT(CASE WHEN service = 'search' THEN 1 END) DESC) AS search_count_percent_rank
FROM parsed
GROUP BY service, CASE WHEN flag_bit2 >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END, level
HAVING COUNT_IF(service = 'billing') > 1;

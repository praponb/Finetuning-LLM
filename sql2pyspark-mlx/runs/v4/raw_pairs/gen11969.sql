WITH parsed AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        payload_json,
        context,
        CAST((status_code & 8) != 0 AS INT) AS has_fault,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier
    FROM app_logs
)
SELECT
    id,
    host,
    log_ts,
    level,
    status_code,
    payload_json,
    context,
    has_fault,
    user_tier,
    SUM(status_code) OVER (PARTITION BY host) host_sum_status_code,
    NTILE(10) OVER (PARTITION BY level ORDER BY has_fault DESC, id) has_fault_bucket,
    SUM(status_code) OVER (PARTITION BY host, level ORDER BY status_code, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_status_code,
    NTILE(5) OVER (ORDER BY status_code, id) status_code_bucket,
    MIN(has_fault) OVER (PARTITION BY level) level_min_has_fault
FROM parsed

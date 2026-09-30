WITH parsed AS (
    SELECT
        id,
        log_ts,
        status_code,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        context['version'] AS version
    FROM app_logs
)
SELECT
    id,
    log_ts,
    status_code_high,
    duration_sec,
    version,
    AVG(status_code) OVER (ORDER BY status_code, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_status_code,
    MIN(status_code) OVER (ORDER BY status_code, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_status_code,
    MAX(status_code) OVER (ORDER BY status_code, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_status_code
FROM parsed;

WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        context,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        COALESCE(context['region'], 'unknown') AS region_or_default
    FROM app_logs
)
-- Ranking and gap features
SELECT
    id,
    log_ts,
    level,
    status_code,
    region_or_default,
    LAST_VALUE(region_or_default, TRUE) OVER w_grp last_known_region_or_default,
    ROUND(status_code / SUM(status_code) OVER (PARTITION BY host, service), 4) status_code_share,
    AVG(status_code_high) OVER (PARTITION BY host) host_avg_status_code_high,
    NTILE(4) OVER (PARTITION BY host ORDER BY status_code DESC, id) status_code_bucket
FROM unpacked
WINDOW w_grp AS (PARTITION BY level ORDER BY status_code_high, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

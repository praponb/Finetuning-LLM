WITH parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        context,
        SIZE(context) AS num_context,
        COALESCE(context['region'], 'none') AS region_or_default,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier
    FROM app_logs
)
-- Running and moving aggregates
SELECT
    id,
    service,
    log_ts,
    latency_ms,
    status_code,
    num_context,
    user_tier,
    ROW_NUMBER() OVER (PARTITION BY service ORDER BY num_context, id) AS row_seq,
    MAX(status_code) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_max_2h,
    NTILE(10) OVER (PARTITION BY service ORDER BY status_code DESC, id) AS status_code_bucket
FROM parsed

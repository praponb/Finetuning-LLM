WITH parsed AS (
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        SIZE(context) AS num_context,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier
    FROM app_logs
    WHERE level IN ('WARN', 'DEBUG', 'INFO')
)
SELECT
    id,
    status_code,
    status_code_high,
    num_context,
    tier,
    AVG(status_code) OVER (ORDER BY num_context, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS status_code_avg_3_prior,
    NTILE(4) OVER (PARTITION BY tier, level ORDER BY latency_ms DESC, id) AS latency_ms_bucket,
    NTILE(5) OVER (PARTITION BY tier ORDER BY status_code_high DESC, id) AS status_code_high_bucket,
    AVG(latency_ms) OVER (PARTITION BY tier, level) AS tier_avg_latency_ms,
    CORR(status_code, latency_ms) OVER (PARTITION BY level) AS corr_status_code_latency_ms
FROM parsed
WHERE log_ts < '2025-10-01';

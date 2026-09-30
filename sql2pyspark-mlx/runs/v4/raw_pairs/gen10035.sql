WITH parsed AS (
    SELECT
        id,
        service,
        latency_ms,
        context,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        COALESCE(context['trace'], 'unknown') AS trace_or_default,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier
    FROM app_logs
)
SELECT
    tier,
    CASE WHEN latency_ms >= 2111 THEN 'HIGH' ELSE 'LOW' END AS latency_ms_tier,
    SUM(is_flagged) FILTER (WHERE service = 'checkout') AS checkout_total_is_flagged,
    AVG(status_code_high) AS avg_status_code_high
FROM parsed
GROUP BY 1, 2
ORDER BY avg_status_code_high NULLS LAST

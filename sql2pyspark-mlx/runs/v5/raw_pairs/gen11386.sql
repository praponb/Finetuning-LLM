WITH parsed AS (
    SELECT
        id,
        host,
        level,
        payload_json,
        context,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        context['trace'] AS trace,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier
    FROM app_logs
)
SELECT
    CASE WHEN status_code_high >= 2 THEN 'LARGE' ELSE 'SMALL' END AS status_code_high_tier,
    PERCENTILE_APPROX(status_code_high, 0.95) AS status_code_high_p95,
    COUNT(DISTINCT tier) AS distinct_tiers,
    ROUND(AVG(status_code_high), 2) AS avg_status_code_high,
    COUNT(*) FILTER (WHERE tier = 'gold') AS gold_rows
FROM parsed
GROUP BY CASE WHEN status_code_high >= 2 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(DISTINCT tier) > 1
ORDER BY gold_rows DESC

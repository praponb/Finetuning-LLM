WITH flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        level,
        payload_json,
        context,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        SIZE(context) AS num_context,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        COALESCE(context['region'], 'unknown') AS region_or_default,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4
    FROM app_logs
    WHERE level NOT IN ('DEBUG', 'WARN', 'ERROR')
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN duration_sec >= 3.3 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_tier,
    CASE WHEN num_context >= 60 THEN 'HIGH' ELSE 'LOW' END AS num_context_tier,
    APPROX_PERCENTILE(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles,
    SIZE(COLLECT_SET(tier)) AS num_tiers,
    ROUND(AVG(num_context), 2) AS avg_num_context,
    RANK() OVER (PARTITION BY CASE WHEN duration_sec >= 3.3 THEN 'LARGE' ELSE 'SMALL' END ORDER BY SIZE(COLLECT_SET(tier)) DESC) AS num_tiers_rank
FROM flattened_attrs
GROUP BY 1, 2
ORDER BY avg_num_context DESC;

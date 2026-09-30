WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        status_code,
        COALESCE(context['region'], 'unknown') AS region_or_default,
        context['version'] AS version,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier
    FROM app_logs
)
-- Window metrics over several different partitions
SELECT
    *,
    LAG(status_code, 1) OVER (PARTITION BY tier ORDER BY status_code, id) AS prev_status_code,
    FIRST_VALUE(version) IGNORE NULLS OVER (PARTITION BY tier, service ORDER BY status_code, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_version
FROM flattened_attrs;

WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        status_code,
        ARRAY_CONTAINS(MAP_KEYS(context), 'trace') AS has_trace,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        context['trace'] AS trace,
        SIZE(context) AS num_context
    FROM app_logs
)
-- Window metrics over several different partitions
SELECT
    id,
    status_code,
    has_trace,
    tier,
    trace,
    LAST_VALUE(num_context, TRUE) OVER (PARTITION BY tier ORDER BY status_code, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_num_context,
    LEAD(status_code, 1) OVER (ORDER BY num_context, id) AS next_status_code,
    STDDEV_SAMP(status_code) OVER (PARTITION BY tier, service) AS tier_stddev_samp_status_code
FROM flattened_attrs

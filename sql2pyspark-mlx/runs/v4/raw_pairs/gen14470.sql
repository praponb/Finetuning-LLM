WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        payload_json,
        context['version'] AS version,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        context['region'] AS region
    FROM app_logs
)
-- Running and moving aggregates
SELECT
    *,
    NTILE(3) OVER w_ordered AS status_code_high_bucket,
    NTILE(5) OVER w_grp AS status_code_bucket
FROM flattened_attrs
WINDOW w_ordered AS (PARTITION BY level ORDER BY status_code_high DESC, id), w_grp AS (ORDER BY status_code, id)

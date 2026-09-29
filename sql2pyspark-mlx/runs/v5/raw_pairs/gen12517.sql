WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        log_ts,
        level,
        payload_json,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        SIZE(context) AS num_context,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1
    FROM app_logs
)
-- Multi-level aggregation
SELECT
    level,
    SORT_ARRAY(COLLECT_SET(service)) AS service_list,
    MAX(num_context) FILTER (WHERE level = 'DEBUG') AS debug_max_num_context
FROM unpacked
GROUP BY level

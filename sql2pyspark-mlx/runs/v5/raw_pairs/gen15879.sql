WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        host,
        level,
        context['version'] AS version,
        context['trace'] AS trace,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id
    FROM app_logs
    WHERE host = 'h3'
)
-- Aggregate with conditional metrics
SELECT
    level,
    SORT_ARRAY(COLLECT_SET(host)) AS host_list,
    COUNT(*) FILTER (WHERE level = 'DEBUG') AS debug_rows,
    COUNT(*) AS row_count,
    COUNT(*) AS cnt,
    GROUPING_ID() AS gid
FROM flattened_attrs
GROUP BY CUBE (level);

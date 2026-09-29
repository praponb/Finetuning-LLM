WITH lines AS (
    SELECT
        id,
        level,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, value
)
SELECT
    id,
    level,
    status_code,
    CASE WHEN level RLIKE '^(DEBUG|WARN)$' THEN 'OTHER_ZONE' WHEN level RLIKE '^(INFO|ERROR)$' THEN 'CORE' ELSE 'GROUP_B' END AS level_zone
FROM lines

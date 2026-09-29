WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, attr_value
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, level
    FROM flattened
    INTERSECT ALL
    SELECT id, level
    FROM app_logs
    WHERE level != 'INFO'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    INNER JOIN matched_keys k ON s.id = k.id AND s.level = k.level
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), log_ts) days_since_log_ts
    FROM kept_rows
    WHERE log_ts > DATE '2025-06-01'
      AND key IS NOT NULL
      AND latency_ms != 797
)
SELECT
    log_ts,
    level,
    latency_ms,
    status_code,
    key,
    attr_value,
    days_since_log_ts,
    CASE WHEN level RLIKE '^(INFO|DEBUG)$' THEN 'GROUP_B' WHEN level RLIKE '^(WARN|ERROR)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS level_zone
FROM cleaned;

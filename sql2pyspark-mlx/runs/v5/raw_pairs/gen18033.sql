WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, attr_value
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(level, 1, 3) AS level_prefix,
        COALESCE(k, 'UNKNOWN') AS k_filled
    FROM exploded
    WHERE k = 'region'
),
valid_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        id,
        level,
        k,
        attr_value,
        LOWER(level) AS level_lower,
        COALESCE(k, 'DIRECT') AS k_filled_2
    FROM valid
    WHERE k IS NOT DISTINCT FROM 'trace'
      AND level IN ('DEBUG', 'ERROR', 'WARN')
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        level,
        level_lower,
        k_filled_2,
        UPPER(k) AS k_upper
    FROM valid_app_logs
    WHERE level IN ('ERROR', 'DEBUG', 'WARN')
),
valid_app_logs_2 AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(level)) AS level_title
    FROM cleaned
    WHERE level = 'WARN'
)
-- Grouped metrics
SELECT
    level,
    COUNT(*) AS event_count,
    COUNT(CASE WHEN level = 'WARN' THEN 1 END) AS warn_count,
    COUNT(CASE WHEN level = 'WARN' THEN 1 END) AS warn_count_2,
    COUNT_IF(level = 'DEBUG') AS debug_events
FROM valid_app_logs_2
GROUP BY CUBE (level)
HAVING COUNT(CASE WHEN level = 'WARN' THEN 1 END) < 1000
ORDER BY event_count DESC NULLS FIRST

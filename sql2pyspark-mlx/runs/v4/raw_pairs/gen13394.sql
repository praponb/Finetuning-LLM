WITH lines AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, value
),
filtered AS (
    SELECT
        *,
        LENGTH(key) key_length,
        COALESCE(key, 'N/A') key_clean
    FROM lines
    WHERE key IS NOT NULL
      AND key = 'region'
),
base AS (
    SELECT
        id,
        log_ts,
        level,
        key,
        value,
        key_clean,
        CONCAT(service, ', ', level) service_level,
        CONCAT(key, ' - ', service) key_service,
        COALESCE(key, 'N/A') resolved_key
    FROM filtered
    WHERE log_ts > DATE '2025-06-01'
      AND key IS NOT NULL
),
valid AS (
    SELECT
        *,
        CONCAT_WS('/', key, level) key_level_key,
        COALESCE(key, 'NONE') resolved_key_2
    FROM base
    WHERE log_ts >= TO_TIMESTAMP('2025-06-01 15:00:00')
      AND level <> 'DEBUG'
),
filtered_app_logs AS (
    SELECT *, CONCAT(level, '/', key) level_key
    FROM valid
    WHERE log_ts > DATE '2025-06-01'
      AND key IS NOT NULL
      AND HOUR(log_ts) BETWEEN 6 AND 13
)
SELECT
    id,
    log_ts,
    level,
    key,
    value,
    key_service,
    resolved_key,
    key_level_key,
    resolved_key_2,
    CASE WHEN level RLIKE '^(INFO|DEBUG)$' THEN 'EXTENDED' WHEN level RLIKE '^(WARN|ERROR)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS level_zone
FROM filtered_app_logs
ORDER BY log_ts;

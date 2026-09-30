WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        value,
        SUBSTRING(host, 1, 3) AS host_prefix
    FROM lines
    WHERE key IS DISTINCT FROM 'region'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        log_ts,
        level,
        value,
        CONCAT(host, ', ', service) AS host_service
    FROM base
    WHERE level NOT IN ('DEBUG', 'INFO', 'WARN')
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        log_ts,
        value,
        host_service,
        CASE WHEN level IN ('INFO', 'DEBUG') THEN 'Tier 1' ELSE 'Other' END AS level_group
    FROM valid
    WHERE level <> 'ERROR'
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    MAX(log_ts) AS ended_at,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
    MAX(log_ts) AS ended_at_2,
    MIN(log_ts) AS started_at,
    MIN(log_ts) AS first_seen,
    MIN(log_ts) AS first_log_ts
FROM cleaned
GROUP BY DATE_TRUNC('HOUR', log_ts)
ORDER BY span_minutes ASC NULLS LAST

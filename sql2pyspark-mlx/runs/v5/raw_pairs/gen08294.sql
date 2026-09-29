WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(host, ' ', level) AS host_level
    FROM exploded
    WHERE key IS NOT NULL
      AND level IN ('INFO', 'DEBUG', 'ERROR')
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        key,
        value,
        SUBSTRING(level, 1, 3) AS level_prefix,
        SUBSTRING(key, 1, 2) AS key_prefix,
        COALESCE(key, 'UNKNOWN') AS key_clean
    FROM filtered
    WHERE key IS NOT NULL
      AND key IN ('trace', 'version', 'region')
),
filtered_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN latency_ms >= 2337 THEN 'High' WHEN latency_ms >= 1161 THEN 'Medium' ELSE 'Low' END AS latency_ms_band,
        COALESCE(key, 'N/A') AS key_clean_2
    FROM base
    WHERE latency_ms <= 2107
),
kept_keys AS (
    -- Set difference on keys
    SELECT service
    FROM filtered_app_logs
    EXCEPT ALL
    SELECT service
    FROM app_logs
    WHERE log_ts < '2025-04-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered_app_logs s
    JOIN kept_keys k ON s.service = k.service
)
SELECT
    id,
    level,
    latency_ms,
    key,
    value,
    level_prefix,
    key_clean_2,
    CASE WHEN key RLIKE '^(version|trace)$' THEN 'PRIORITY' WHEN key RLIKE '^(region)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS key_zone
FROM kept_rows
ORDER BY key;

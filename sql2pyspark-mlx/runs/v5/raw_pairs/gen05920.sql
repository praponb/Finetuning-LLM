WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, v
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        service,
        host,
        log_ts,
        level,
        key,
        v,
        UPPER(level) AS level_upper,
        SUBSTRING(host, 1, 2) AS host_prefix
    FROM flattened
    WHERE key IS DISTINCT FROM 'version'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS('-', service, key) AS service_key_key,
        COALESCE(key, 'Unknown') AS key_filled
    FROM filtered
    WHERE service = 'auth'
      AND log_ts > TIMESTAMP '2025-06-01 14:00:00'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(host) AS host_length
    FROM cleaned
    WHERE key IS DISTINCT FROM 'trace'
      AND key IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT *, AVG(host_length) OVER (PARTITION BY level) AS level_avg_host_length
    FROM valid
    WHERE host_length <= 39
),
stats AS (
    -- Multi-level aggregation
    SELECT
        level,
        SUM(CASE WHEN level = 'WARN' THEN level_avg_host_length ELSE 0 END) AS warn_level_avg_host_length,
        MIN(log_ts) AS first_log_ts,
        GROUPING(level) AS grouping_level
    FROM enriched
    GROUP BY CUBE (level)
)
SELECT warn_level_avg_host_length, first_log_ts
FROM stats
ORDER BY warn_level_avg_host_length DESC NULLS FIRST

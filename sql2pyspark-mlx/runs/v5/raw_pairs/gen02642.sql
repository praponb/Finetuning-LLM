WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, value
),
cleaned AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        key,
        value,
        CEIL(status_code) status_code_whole
    FROM flattened
    WHERE log_ts >= TIMESTAMP '2025-06-01 07:00:00'
      AND level IN ('WARN', 'INFO', 'DEBUG')
),
ranked AS (
    SELECT
        id,
        level,
        latency_ms,
        key,
        value,
        status_code_whole
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY host ORDER BY status_code_whole ASC, id ASC, key ASC) = 1
),
base AS (
    SELECT
        id,
        level,
        latency_ms,
        key,
        status_code_whole,
        LOWER(key) key_lower,
        INITCAP(TRIM(level)) level_title,
        COALESCE(key, 'UNKNOWN') key_filled
    FROM ranked
    WHERE key IS NOT NULL
),
scored AS (
    SELECT
        *,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY key), 4) latency_ms_share,
        FIRST_VALUE(key) IGNORE NULLS OVER (ORDER BY status_code_whole ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_key
    FROM base
)
SELECT
    level,
    AVG(status_code_whole) FILTER (WHERE key = 'version') AS version_avg_status_code_whole,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list,
    COUNT(*) AS row_count
FROM scored
GROUP BY level
ORDER BY row_count ASC NULLS FIRST;

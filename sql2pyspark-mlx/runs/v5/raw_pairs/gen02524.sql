WITH lines AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, v
),
cleaned AS (
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        key,
        CAST(latency_ms AS STRING) AS latency_ms_num,
        COALESCE(key, 'NONE') AS key_filled
    FROM lines
    WHERE key IS DISTINCT FROM 'region'
      AND log_ts > TIMESTAMP '2025-06-01 11:00:00'
),
ranked AS (
    SELECT
        id,
        key,
        latency_ms_num
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY host, level ORDER BY status_code ASC, id ASC, key ASC) = 1
),
filtered AS (
    SELECT *, INITCAP(TRIM(key)) AS key_title
    FROM ranked
    WHERE key IN ('version', 'region')
)
SELECT
    key,
    COUNT_IF(key = 'region') AS region_events,
    COUNT(CASE WHEN key = 'region' THEN 1 END) AS region_count,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list,
    COUNT(*) FILTER (WHERE key = 'region') AS region_rows
FROM filtered
GROUP BY key
HAVING COUNT(CASE WHEN key = 'region' THEN 1 END) < 1000;

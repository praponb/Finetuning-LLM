WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, attr_value
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        host,
        level,
        latency_ms,
        attr_key,
        attr_value,
        LOWER(host) AS host_lower
    FROM flattened
    WHERE attr_key IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(latency_ms, TRUE) OVER w_running AS last_known_latency_ms,
        SUM(latency_ms) OVER w_running AS running_latency_ms,
        SUM(latency_ms) OVER (PARTITION BY host, attr_key ORDER BY latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_latency_ms
    FROM cleaned
    WINDOW w_running AS (PARTITION BY attr_key ORDER BY latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(latency_ms AS STRING) AS latency_ms_str,
        COALESCE(attr_key, 'OTHER') AS resolved_attr_key
    FROM enriched
    WHERE running_latency_ms <= 3372
)
SELECT
    id,
    host,
    level,
    attr_key,
    attr_value,
    last_known_latency_ms,
    running_latency_ms,
    cumulative_latency_ms,
    latency_ms_str
FROM valid
ORDER BY running_latency_ms DESC NULLS FIRST, level;

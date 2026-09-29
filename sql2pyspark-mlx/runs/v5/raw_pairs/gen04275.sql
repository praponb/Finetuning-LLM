WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, value
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        level,
        latency_ms,
        attr_key,
        value,
        LAG(latency_ms) OVER w_running AS prev_latency_ms,
        LEAD(latency_ms) OVER w_running AS next_latency_ms,
        ROW_NUMBER() OVER w_running AS seq_no
    FROM lines
    WINDOW w_running AS (PARTITION BY level ORDER BY latency_ms, id, attr_key)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        level,
        attr_key,
        value,
        next_latency_ms,
        seq_no,
        next_latency_ms - prev_latency_ms AS next_latency_ms_minus_prev_latency_ms,
        COALESCE(latency_ms, 0) AS latency_ms_filled
    FROM metrics
    WHERE attr_key IS DISTINCT FROM 'region'
      AND level IN ('WARN', 'DEBUG', 'ERROR')
),
metrics_app_logs AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(latency_ms_filled) OVER (PARTITION BY level ORDER BY latency_ms_filled, id, attr_key) AS next_latency_ms_filled,
        LAG(latency_ms_filled) OVER (PARTITION BY level ORDER BY latency_ms_filled, id, attr_key) AS prev_latency_ms_filled,
        LEAD(next_latency_ms) OVER (PARTITION BY level ORDER BY latency_ms_filled, id, attr_key) AS next_next_latency_ms,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY latency_ms_filled, id, attr_key) AS event_seq
    FROM valid
)
SELECT
    id,
    level,
    attr_key,
    value,
    next_latency_ms,
    prev_latency_ms_filled,
    CASE WHEN level RLIKE '^(ERROR|WARN)$' THEN 'GROUP_A' WHEN level RLIKE '^(INFO|DEBUG)$' THEN 'CORE' ELSE 'EXTENDED' END AS level_zone
FROM metrics_app_logs
ORDER BY next_latency_ms;

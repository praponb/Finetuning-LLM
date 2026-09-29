WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        latency_ms,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, v
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM exploded
    EXCEPT ALL
    SELECT id
    FROM app_logs
    WHERE level IN ('DEBUG', 'WARN')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    JOIN remaining_keys k ON s.id = k.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        latency_ms,
        attr_key,
        v,
        CASE WHEN attr_key = 'trace' THEN 1 ELSE 0 END is_trace
    FROM verified
    WHERE attr_key IS NOT NULL
      AND attr_key IS DISTINCT FROM 'trace'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        latency_ms,
        attr_key,
        v,
        UPPER(attr_key) attr_key_upper
    FROM base
    WHERE is_trace >= 52
)
-- Grouped metrics
SELECT
    attr_key,
    AVG(latency_ms) FILTER (WHERE attr_key = 'version') AS version_avg_latency_ms,
    COUNT_IF(attr_key = 'trace') AS trace_events,
    BIT_AND(latency_ms) AS latency_ms_bit_and
FROM filtered
GROUP BY attr_key
HAVING COUNT_IF(attr_key = 'trace') > 1
ORDER BY version_avg_latency_ms DESC

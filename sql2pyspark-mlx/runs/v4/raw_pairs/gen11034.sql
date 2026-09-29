WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, attr_value
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(service) AS service_upper,
        COALESCE(key, 'UNKNOWN') AS key_filled
    FROM lines
    WHERE log_ts > DATE '2025-06-01'
      AND key IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, service
    FROM valid
    EXCEPT
    SELECT id, service
    FROM app_logs
    WHERE HOUR(log_ts) BETWEEN 8 AND 16
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN remaining_keys k ON s.id = k.id AND s.service = k.service
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            host,
            service,
            level,
            latency_ms
        FROM verified
    ) src
    PIVOT (
        SUM(latency_ms)
        FOR level IN ('WARN' AS warn, 'ERROR' AS error)
    )
)
SELECT host, service
FROM pivoted
ORDER BY host NULLS LAST

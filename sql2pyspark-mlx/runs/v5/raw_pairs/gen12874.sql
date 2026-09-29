WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, v
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        host,
        latency_ms,
        key,
        CASE WHEN key = 'region' THEN 1 ELSE 0 END AS is_region,
        COALESCE(key, 'UNKNOWN') AS key_filled
    FROM lines
    WHERE latency_ms = 857
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY key, service ORDER BY latency_ms DESC, id, key) AS row_num
    FROM base
    QUALIFY row_num = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT host, id
    FROM latest
    EXCEPT ALL
    SELECT host, id
    FROM app_logs
    WHERE service IN ('search', 'auth', 'checkout')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.host = k.host AND s.id = k.id
)
SELECT
    id,
    service,
    latency_ms,
    key,
    row_num
FROM verified
ORDER BY service DESC;

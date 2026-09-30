WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, attr_value
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        k,
        attr_value,
        LOWER(host) AS host_lower,
        COALESCE(k, 'DIRECT') AS k_filled
    FROM exploded
    WHERE host = 'h1'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT *, DENSE_RANK() OVER (PARTITION BY host ORDER BY latency_ms DESC) AS latency_ms_dense_rank
    FROM valid
),
valid_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        host,
        latency_ms,
        status_code,
        k,
        latency_ms_dense_rank,
        INITCAP(TRIM(k)) AS k_title,
        COALESCE(k, 'OTHER') AS k_filled_2
    FROM metrics
    WHERE latency_ms_dense_rank BETWEEN 4 AND 6
)
SELECT
    host,
    latency_ms,
    status_code,
    k,
    k_title,
    CASE WHEN k RLIKE '^(region|trace)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS k_zone
FROM valid_app_logs
ORDER BY latency_ms DESC

WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, value
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        k,
        value,
        LAG(latency_ms, 1) OVER w_grp AS prev_latency_ms,
        NTILE(4) OVER w_grp_2 AS status_code_bucket
    FROM exploded
    WHERE k = 'version'
    WINDOW w_grp AS (ORDER BY status_code ASC, id ASC, k ASC), w_grp_2 AS (PARTITION BY service, k ORDER BY status_code ASC, id ASC, k ASC)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        service,
        status_code,
        k,
        prev_latency_ms,
        status_code_bucket
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY service, k ORDER BY status_code DESC, id ASC, k ASC) AS dedup_rank
        FROM metrics
    ) t
    WHERE dedup_rank = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        status_code,
        k,
        CASE WHEN status_code_bucket >= 9 THEN 'High' WHEN status_code_bucket >= 3 THEN 'Medium' ELSE 'Low' END AS status_code_bucket_level,
        CASE WHEN k = 'region' THEN 1 ELSE 0 END AS is_region,
        COALESCE(k, 'NONE') AS k_filled
    FROM latest
    WHERE status_code > 263
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM base
    EXCEPT DISTINCT
    SELECT id
    FROM app_logs
    WHERE latency_ms <> 816
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.id = k.id
)
SELECT
    status_code,
    k,
    status_code_bucket_level,
    is_region,
    k_filled,
    CASE WHEN k RLIKE '^(version|region)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS k_zone
FROM verified
ORDER BY k_zone ASC

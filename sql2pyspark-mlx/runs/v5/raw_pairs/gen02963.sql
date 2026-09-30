WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, v
),
parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        status_code,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS has_fault
    FROM exploded
    WHERE status_code <= 375
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, CEIL(status_code_high) status_code_high_whole
    FROM parsed
    WHERE status_code_high <= 2
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM base
    INTERSECT ALL
    SELECT id
    FROM app_logs
    WHERE level = 'ERROR'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.id = k.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        status_code,
        status_code_high,
        has_fault,
        ROUND(status_code_high, 1) status_code_high_rounded
    FROM reconciled
    WHERE has_fault = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT *, DENSE_RANK() OVER (ORDER BY status_code) status_code_dense_rank
    FROM filtered
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_code_high >= 2 THEN 'HIGH' ELSE 'LOW' END AS status_code_high_tier,
    ROUND(AVG(status_code), 2) AS avg_status_code,
    SUM(status_code_high_rounded) AS total_status_code_high_rounded,
    APPROX_PERCENTILE(has_fault, ARRAY(0.25, 0.5, 0.75)) AS has_fault_quartiles,
    PERCENTILE_APPROX(status_code_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS status_code_dense_rank_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS num_rows
FROM with_windows
GROUP BY 1
HAVING SUM(status_code_high_rounded) >= 0

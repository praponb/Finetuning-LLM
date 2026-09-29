WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT host, id
    FROM exploded
    INTERSECT ALL
    SELECT host, id
    FROM app_logs
    WHERE latency_ms <= 2325
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    INNER JOIN remaining_keys k ON s.host = k.host AND s.id = k.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(host) AS host_lower,
        COALESCE(k, 'NONE') AS k_clean
    FROM kept_rows
    WHERE k IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        v,
        host_lower,
        k_clean,
        CORR(latency_ms, status_code) OVER (PARTITION BY host) AS corr_latency_ms_status_code,
        CORR(latency_ms, status_code) OVER (PARTITION BY k) AS corr_latency_ms_status_code_2,
        CORR(status_code, latency_ms) OVER (PARTITION BY k) AS corr_status_code_latency_ms,
        AVG(latency_ms) OVER (PARTITION BY k, host) AS k_avg_latency_ms,
        CORR(latency_ms, status_code) OVER (PARTITION BY k) AS corr_latency_ms_status_code_3
    FROM filtered
    WHERE status_code > 404
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        v,
        k_clean,
        corr_latency_ms_status_code,
        corr_latency_ms_status_code_2,
        corr_status_code_latency_ms,
        k_avg_latency_ms,
        corr_latency_ms_status_code_3,
        CAST(corr_status_code_latency_ms AS INT) AS corr_status_code_latency_ms_int,
        GREATEST(corr_latency_ms_status_code, corr_latency_ms_status_code_2) AS greatest_corr_latency_ms_status_code_corr_latency_ms_status_code_2
    FROM metrics
    WHERE corr_latency_ms_status_code_2 BETWEEN 0.0 AND 0.2
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN corr_status_code_latency_ms_int >= 83 THEN 'High' WHEN corr_status_code_latency_ms_int >= 36 THEN 'Medium' ELSE 'Low' END AS corr_status_code_latency_ms_int_band
    FROM valid
    WHERE k_avg_latency_ms >= 1852
)
-- Grouped metrics
SELECT
    CASE WHEN corr_latency_ms_status_code_2 >= 0.7 THEN 'HIGH' ELSE 'LOW' END AS corr_latency_ms_status_code_2_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(greatest_corr_latency_ms_status_code_corr_latency_ms_status_code_2) AS avg_greatest_corr_latency_ms_status_code_corr_latency_ms_status_code_2,
    PERCENTILE_APPROX(corr_latency_ms_status_code, 0.95) AS corr_latency_ms_status_code_p95
FROM base
GROUP BY corr_latency_ms_status_code_2_tier
ORDER BY distinct_ids DESC NULLS FIRST

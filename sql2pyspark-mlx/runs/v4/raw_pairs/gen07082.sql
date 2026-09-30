WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, v
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(latency_ms / status_code, 2) latency_ms_per_status_code,
        TO_DATE(log_ts) log_ts_date
    FROM flattened
    WHERE k IS NOT NULL
      AND log_ts < '2025-05-05'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(latency_ms, 2) OVER (ORDER BY log_ts_date, id, k) prev_latency_ms,
        SUM(latency_ms) OVER (ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_latency_ms
    FROM filtered
)
-- Aggregate with conditional metrics
SELECT
    k,
    APPROX_PERCENTILE(status_code, ARRAY(0.25, 0.5, 0.75)) AS status_code_quartiles,
    BIT_AND(latency_ms) AS latency_ms_bit_and,
    BIT_AND(status_code) AS status_code_bit_and,
    SUM(latency_ms_per_status_code) AS total_latency_ms_per_status_code
FROM with_windows
GROUP BY k;

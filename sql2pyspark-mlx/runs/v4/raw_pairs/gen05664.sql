WITH exploded AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, v
),
cleaned AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        ROUND(latency_ms * 1.15, 2) AS adjusted_latency_ms,
        DATE_FORMAT(log_ts, 'yyyy-MM') AS log_ts_label,
        COALESCE(k, 'UNKNOWN') AS k_filled
    FROM exploded
    WHERE log_ts >= DATE '2025-06-01'
      AND k IS NOT NULL
      AND HOUR(log_ts) BETWEEN 9 AND 14
),
filtered AS (
    SELECT
        id,
        latency_ms,
        log_ts_label,
        k_filled,
        CAST(adjusted_latency_ms AS INT) AS adjusted_latency_ms_int
    FROM cleaned
    WHERE log_ts > TIMESTAMP '2025-06-01 06:00:00'
),
daily AS (
    SELECT
        CASE WHEN adjusted_latency_ms_int >= 64 THEN 'HIGH' ELSE 'LOW' END AS adjusted_latency_ms_int_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT id) AS distinct_ids_2,
        COUNT(DISTINCT id) AS distinct_ids_3,
        GROUPING(CASE WHEN adjusted_latency_ms_int >= 64 THEN 'HIGH' ELSE 'LOW' END) AS grouping_level
    FROM filtered
    GROUP BY CUBE (CASE WHEN adjusted_latency_ms_int >= 64 THEN 'HIGH' ELSE 'LOW' END)
)
SELECT
    adjusted_latency_ms_int_tier,
    distinct_ids_2,
    distinct_ids_3,
    grouping_level
FROM daily

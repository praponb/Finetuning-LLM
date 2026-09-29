WITH lines AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, attr_value
),
filtered AS (
    SELECT
        latency_ms,
        attr_value,
        SUBSTRING(k, 1, 4) AS k_prefix
    FROM lines
    WHERE log_ts >= TIMESTAMP '2025-06-01 06:00:00'
      AND k IS NOT NULL
),
valid AS (
    SELECT *, ROUND(latency_ms * 1.07, 2) AS latency_ms_adjusted
    FROM filtered
    WHERE latency_ms < 1202
),
cleaned AS (
    SELECT
        *,
        ROUND(latency_ms_adjusted, 0) AS latency_ms_adjusted_rounded,
        COALESCE(latency_ms, 0) AS latency_ms_filled
    FROM valid
    WHERE latency_ms_adjusted BETWEEN 67 AND 76
),
with_windows AS (
    SELECT
        latency_ms,
        attr_value,
        k_prefix,
        CUME_DIST() OVER (ORDER BY latency_ms_adjusted DESC NULLS LAST) AS latency_ms_adjusted_cume_dist,
        DENSE_RANK() OVER (ORDER BY latency_ms_filled DESC NULLS LAST) AS latency_ms_filled_dense_rank
    FROM cleaned
    WHERE latency_ms_filled <= 41
)
SELECT
    CASE WHEN latency_ms >= 2437 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_tier,
    PERCENTILE_APPROX(latency_ms_filled_dense_rank, 0.9) AS latency_ms_filled_dense_rank_p90,
    PERCENTILE_APPROX(latency_ms, 0.95) AS latency_ms_p95,
    COUNT(*) AS num_rows,
    MAX(latency_ms) AS max_latency_ms
FROM with_windows
GROUP BY 1
HAVING latency_ms_p95 >= 0

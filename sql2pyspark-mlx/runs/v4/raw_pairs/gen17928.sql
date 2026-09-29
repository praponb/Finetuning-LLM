WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, v
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        latency_ms,
        k,
        TRIM(level) AS clean_level,
        COALESCE(k, 'DIRECT') AS k_filled
    FROM exploded
    WHERE k IS NOT NULL
      AND host IN ('h3', 'h4', 'h2')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(latency_ms) OVER w_part AS host_avg_latency_ms,
        NTILE(10) OVER w_part_2 AS latency_ms_bucket,
        RANK() OVER (PARTITION BY k ORDER BY latency_ms) AS latency_ms_rank
    FROM valid
    WINDOW w_part AS (PARTITION BY host), w_part_2 AS (PARTITION BY host, k ORDER BY latency_ms, id, k)
)
-- Multi-level aggregation
SELECT
    CASE WHEN latency_ms >= 2489 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_tier,
    host,
    ROUND(SUM(latency_ms_bucket) / NULLIF(COUNT(CASE WHEN k = 'trace' THEN 1 END), 0), 2) AS latency_ms_bucket_per_trace,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list,
    COUNT(CASE WHEN host = 'h1' THEN 1 END) AS h1_count,
    COUNT(*) FILTER (WHERE k = 'trace') AS trace_rows,
    PERCENTILE_APPROX(latency_ms, 0.9) AS latency_ms_p90
FROM scored
GROUP BY CASE WHEN latency_ms >= 2489 THEN 'PREMIUM' ELSE 'STANDARD' END, host

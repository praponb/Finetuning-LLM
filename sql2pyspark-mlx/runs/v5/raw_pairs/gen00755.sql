WITH exploded AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, v
),
metrics AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        k,
        STDDEV_SAMP(latency_ms) OVER (PARTITION BY level, k) level_stddev_samp_latency_ms,
        SUM(latency_ms) OVER (PARTITION BY service ORDER BY latency_ms ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_latency_ms,
        SUM(latency_ms) OVER (PARTITION BY host ORDER BY latency_ms ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_latency_ms_2
    FROM exploded
),
valid AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        k,
        level_stddev_samp_latency_ms,
        cumulative_latency_ms,
        CAST(latency_ms AS DOUBLE) latency_ms_num,
        COALESCE(k, 'UNKNOWN') resolved_k
    FROM metrics
    WHERE k IS NOT NULL
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY host ORDER BY level_stddev_samp_latency_ms DESC NULLS FIRST, id ASC, k ASC) AS latest_rank
        FROM valid
    ) x
    WHERE latest_rank = 1
),
ranked AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        k,
        level_stddev_samp_latency_ms
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY level_stddev_samp_latency_ms ASC, id ASC, k ASC) AS latest_rank_2
        FROM deduped
    ) t
    WHERE latest_rank_2 = 1
)
SELECT
    host,
    CASE WHEN latency_ms >= 2624 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_tier,
    SUM(CASE WHEN level = 'ERROR' THEN latency_ms ELSE 0 END) AS error_latency_ms,
    COUNT(CASE WHEN k = 'trace' THEN 1 END) AS trace_count,
    COUNT(CASE WHEN level = 'DEBUG' THEN 1 END) AS debug_count,
    ROUND(SUM(latency_ms) / NULLIF(COUNT(CASE WHEN host = 'h4' THEN 1 END), 0), 2) AS latency_ms_per_h4,
    GROUPING_ID() AS grouping_id
FROM ranked
GROUP BY ROLLUP (host, CASE WHEN latency_ms >= 2624 THEN 'LARGE' ELSE 'SMALL' END)
HAVING latency_ms_per_h4 < 1000000;

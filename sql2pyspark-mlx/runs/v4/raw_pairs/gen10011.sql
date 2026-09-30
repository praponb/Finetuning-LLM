WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, attr_value
),
enriched AS (
    SELECT
        *,
        PERCENT_RANK() OVER w_recent AS latency_ms_percent_rank,
        STDDEV_SAMP(latency_ms) OVER w_recent_2 AS k_stddev_samp_latency_ms
    FROM exploded
    WINDOW w_recent AS (PARTITION BY level ORDER BY latency_ms), w_recent_2 AS (PARTITION BY k)
),
metrics AS (
    SELECT
        *,
        STDDEV_SAMP(latency_ms_percent_rank) OVER (PARTITION BY service, level) AS service_stddev_samp_latency_ms_percent_rank,
        COUNT(k_stddev_samp_latency_ms) OVER (PARTITION BY service) AS service_count_k_stddev_samp_latency_ms,
        CUME_DIST() OVER (PARTITION BY k, level ORDER BY latency_ms_percent_rank DESC) AS latency_ms_percent_rank_cume_dist
    FROM enriched
),
cleaned AS (
    SELECT *, service_count_k_stddev_samp_latency_ms + service_stddev_samp_latency_ms_percent_rank AS service_count_k_stddev_samp_latency_ms_plus_service_stddev_samp_latency_ms_percent_rank
    FROM metrics
    WHERE log_ts >= TIMESTAMP '2025-06-01 10:00:00'
      AND k IS NOT NULL
)
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    SUM(service_stddev_samp_latency_ms_percent_rank) AS total_service_stddev_samp_latency_ms_percent_rank,
    ROUND(AVG(k_stddev_samp_latency_ms), 2) AS avg_k_stddev_samp_latency_ms,
    COUNT(*) FILTER (WHERE level = 'DEBUG') AS debug_rows
FROM cleaned
GROUP BY DATE_TRUNC('DAY', log_ts)
HAVING SUM(service_stddev_samp_latency_ms_percent_rank) < 0
ORDER BY activity_day NULLS LAST;

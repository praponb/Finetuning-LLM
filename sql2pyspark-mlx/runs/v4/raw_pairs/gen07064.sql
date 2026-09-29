WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, value
),
enriched AS (
    SELECT
        *,
        AVG(latency_ms) OVER (ORDER BY latency_ms, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS latency_ms_avg_3,
        FIRST_VALUE(latency_ms) IGNORE NULLS OVER (PARTITION BY k ORDER BY latency_ms, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_latency_ms
    FROM exploded
),
enriched_app_logs AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY k ORDER BY latency_ms_avg_3 NULLS FIRST) AS latency_ms_avg_3_cume_dist,
        LAG(latency_ms_avg_3, 1) OVER (PARTITION BY level ORDER BY log_ts, id, k) AS prev_latency_ms_avg_3,
        SUM(latency_ms_avg_3) OVER (PARTITION BY service ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_latency_ms_avg_3
    FROM enriched
)
SELECT
    id,
    service,
    log_ts,
    level,
    latency_ms,
    k,
    value,
    latency_ms_avg_3,
    first_known_latency_ms
FROM enriched_app_logs
ORDER BY id, latency_ms_avg_3

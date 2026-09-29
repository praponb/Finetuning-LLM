WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, value
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER w_running AS latency_ms_bucket,
        SUM(latency_ms) OVER w_running_2 AS running_latency_ms,
        NTILE(5) OVER (PARTITION BY host ORDER BY status_code, id, k) AS status_code_bucket
    FROM exploded
    WINDOW w_running AS (ORDER BY latency_ms, id, k), w_running_2 AS (PARTITION BY host ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(latency_ms_bucket) OVER w_part AS cumulative_latency_ms_bucket,
        CUME_DIST() OVER (PARTITION BY k, service ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_cume_dist
    FROM metrics
    WINDOW w_part AS (PARTITION BY service, k ORDER BY running_latency_ms, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY service ORDER BY status_code_bucket DESC, id, k) AS rn
    FROM enriched
    QUALIFY rn = 1
)
SELECT
    id,
    log_ts,
    latency_ms,
    k,
    latency_ms_bucket,
    running_latency_ms,
    status_code_bucket,
    cumulative_latency_ms_bucket,
    latency_ms_cume_dist
FROM deduped
ORDER BY k NULLS FIRST

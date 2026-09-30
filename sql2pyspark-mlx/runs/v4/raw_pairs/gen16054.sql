WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, value
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        latency_ms,
        status_code,
        k,
        value,
        LEAD(latency_ms, 1) OVER (PARTITION BY k ORDER BY latency_ms, id, k) AS next_latency_ms,
        CORR(status_code, latency_ms) OVER (PARTITION BY k) AS corr_status_code_latency_ms,
        CUME_DIST() OVER (PARTITION BY k ORDER BY status_code DESC) AS status_code_cume_dist
    FROM exploded
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY next_latency_ms, id, k) AS rank_in_group
        FROM metrics
    ) x
    WHERE rank_in_group = 1
),
grouped AS (
    -- Grouped metrics
    SELECT
        k,
        SUM(CASE WHEN k = 'trace' THEN next_latency_ms ELSE 0 END) AS trace_next_latency_ms,
        MAX(status_code_cume_dist) AS max_status_code_cume_dist,
        BIT_AND(latency_ms) AS latency_ms_bit_and,
        SIZE(COLLECT_SET(k)) AS num_ks
    FROM latest
    GROUP BY k
)
SELECT
    k,
    trace_next_latency_ms,
    latency_ms_bit_and,
    num_ks,
    CASE WHEN k RLIKE '^(version|region)$' THEN 'PRIORITY' WHEN k RLIKE '^(trace)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS k_zone
FROM grouped
ORDER BY trace_next_latency_ms DESC NULLS LAST, k DESC;

WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, value
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN k IN ('trace', 'version') THEN 'Group A' ELSE 'Other' END AS k_group
    FROM exploded
    WHERE log_ts >= DATE '2025-06-01'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY host, k ORDER BY latency_ms DESC, id, k) AS rn
    FROM valid
    QUALIFY rn = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        value,
        k_group,
        rn,
        status_code + latency_ms AS status_code_plus_latency_ms
    FROM latest
    WHERE k IS NOT NULL
      AND HOUR(log_ts) BETWEEN 12 AND 18
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(latency_ms, 1) OVER (ORDER BY rn, id, k) AS next_latency_ms,
        AVG(status_code) OVER (PARTITION BY host ORDER BY log_ts, id, k ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS status_code_avg_5,
        AVG(status_code) OVER w_recent AS status_code_avg_15m
    FROM base
    WINDOW w_recent AS (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(status_code_avg_5) OVER (PARTITION BY k ORDER BY status_code_plus_latency_ms, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_status_code_avg_5,
        COUNT(status_code_avg_5) OVER (PARTITION BY k ORDER BY status_code_plus_latency_ms, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_status_code_avg_5,
        MAX(status_code_avg_5) OVER (PARTITION BY k ORDER BY status_code_plus_latency_ms, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_status_code_avg_5
    FROM metrics
    WHERE status_code_avg_5 = 402
)
SELECT
    id,
    host,
    k,
    k_group,
    status_code_plus_latency_ms,
    next_latency_ms,
    status_code_avg_5,
    status_code_avg_15m
FROM enriched
ORDER BY host

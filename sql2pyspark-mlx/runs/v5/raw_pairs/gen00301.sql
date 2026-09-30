WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(latency_ms) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_max_15m,
        AVG(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS latency_ms_avg_30m,
        AVG(latency_ms) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_avg_15m,
        COUNT(latency_ms) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS latency_ms_count_30m,
        AVG(latency_ms) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS latency_ms_avg_2h
    FROM lines
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        latency_ms_avg_15m - latency_ms AS latency_ms_avg_15m_minus_latency_ms,
        COALESCE(key, 'NONE') AS resolved_key
    FROM scored
    WHERE log_ts > TO_DATE('2025-06-01')
      AND service IN ('search', 'checkout', 'billing')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        service,
        host,
        log_ts,
        key,
        value,
        latency_ms_max_15m,
        latency_ms_avg_30m,
        latency_ms_avg_2h,
        latency_ms_avg_15m_minus_latency_ms,
        resolved_key,
        ROUND(latency_ms_max_15m / SUM(latency_ms_max_15m) OVER (PARTITION BY service, level), 4) AS latency_ms_max_15m_share
    FROM filtered
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(latency_ms_max_15m) OVER (PARTITION BY service) AS service_sum_latency_ms_max_15m,
        ROUND(latency_ms_avg_2h / SUM(latency_ms_avg_2h) OVER (PARTITION BY key), 4) AS latency_ms_avg_2h_share,
        COUNT(latency_ms_max_15m_share) OVER (PARTITION BY key) AS key_count_latency_ms_max_15m_share,
        PERCENT_RANK() OVER (ORDER BY latency_ms_avg_15m_minus_latency_ms NULLS LAST) AS latency_ms_avg_15m_minus_latency_ms_percent_rank,
        RANK() OVER (PARTITION BY host ORDER BY latency_ms_max_15m_share DESC NULLS FIRST) AS latency_ms_max_15m_share_rank
    FROM with_windows
    WHERE latency_ms_avg_30m >= 708
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(latency_ms_avg_2h / latency_ms_max_15m, 2) AS latency_ms_avg_2h_per_latency_ms_max_15m
    FROM metrics
    WHERE latency_ms_avg_2h_share IS NOT NULL
)
-- Grouped metrics
SELECT
    service,
    MIN(log_ts) AS first_seen,
    APPROX_PERCENTILE(service_sum_latency_ms_max_15m, ARRAY(0.25, 0.5, 0.75)) AS service_sum_latency_ms_max_15m_quartiles
FROM base
GROUP BY 1

WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, attr_value
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(latency_ms, 2) AS latency_ms_rounded,
        COALESCE(k, 'DIRECT') AS k_clean
    FROM exploded
    WHERE k IS NOT NULL
      AND latency_ms <= 696
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        attr_value,
        latency_ms_rounded,
        k_clean,
        LENGTH(k) AS k_length
    FROM cleaned
    WHERE log_ts >= DATE '2025-06-01'
      AND k IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        host,
        status_code,
        latency_ms_rounded,
        k_clean,
        k_length,
        MAX(latency_ms_rounded) OVER (PARTITION BY host) AS host_max_latency_ms_rounded,
        DENSE_RANK() OVER (ORDER BY latency_ms_rounded DESC NULLS FIRST) AS latency_ms_rounded_dense_rank,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY host), 4) AS status_code_share,
        PERCENT_RANK() OVER (PARTITION BY host ORDER BY k_length ASC NULLS LAST) AS k_length_percent_rank
    FROM valid
    WHERE service IN ('auth', 'search', 'billing')
)
SELECT
    id,
    status_code,
    latency_ms_rounded,
    k_length,
    latency_ms_rounded_dense_rank,
    k_length_percent_rank
FROM enriched
ORDER BY k_length_percent_rank DESC NULLS LAST, id ASC;

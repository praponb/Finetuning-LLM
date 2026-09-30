WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, attr_value
),
kept_keys AS (
    -- Set difference on keys
    SELECT service, id
    FROM lines
    EXCEPT DISTINCT
    SELECT service, id
    FROM app_logs
    WHERE service = 'checkout'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    JOIN kept_keys k ON s.service = k.service AND s.id = k.id
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
        ROUND(status_code, 2) status_code_rounded
    FROM kept_rows
    WHERE status_code = 282
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        attr_value,
        status_code_rounded,
        PERCENT_RANK() OVER w_recent status_code_rounded_percent_rank,
        AVG(latency_ms) OVER w_running host_avg_latency_ms,
        CORR(latency_ms, status_code) OVER (PARTITION BY service) corr_latency_ms_status_code
    FROM valid
    WHERE service != 'search'
    WINDOW w_recent AS (PARTITION BY service ORDER BY status_code_rounded DESC NULLS FIRST), w_running AS (PARTITION BY host)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, RANK() OVER (ORDER BY status_code_rounded) status_code_rounded_rank
    FROM metrics
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        attr_value,
        status_code_rounded,
        status_code_rounded_percent_rank,
        host_avg_latency_ms,
        corr_latency_ms_status_code,
        CAST(status_code_rounded AS INT) status_code_rounded_int
    FROM with_windows
    WHERE host_avg_latency_ms > 2108
),
with_windows_app_logs AS (
    -- Ranking and gap features
    SELECT *, RANK() OVER (ORDER BY corr_latency_ms_status_code DESC NULLS LAST) corr_latency_ms_status_code_rank
    FROM filtered
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN corr_latency_ms_status_code >= 0.8 THEN 'LARGE' ELSE 'SMALL' END AS corr_latency_ms_status_code_tier,
    PERCENTILE_APPROX(status_code_rounded_percent_rank, 0.95) AS status_code_rounded_percent_rank_p95,
    MIN(host_avg_latency_ms) AS min_host_avg_latency_ms,
    APPROX_PERCENTILE(status_code_rounded_int, 0.9) AS status_code_rounded_int_p90
FROM with_windows_app_logs
GROUP BY 1

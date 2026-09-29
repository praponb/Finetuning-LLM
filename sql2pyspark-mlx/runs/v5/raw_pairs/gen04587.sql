WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, value
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        host,
        STDDEV_SAMP(status_code) OVER (PARTITION BY host) AS host_stddev_samp_status_code,
        DENSE_RANK() OVER (ORDER BY latency_ms NULLS FIRST) AS latency_ms_dense_rank,
        CUME_DIST() OVER (ORDER BY status_code DESC) AS status_code_cume_dist,
        CORR(latency_ms, status_code) OVER (PARTITION BY host) AS corr_latency_ms_status_code,
        PERCENT_RANK() OVER (ORDER BY status_code DESC) AS status_code_percent_rank
    FROM lines
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ABS(corr_latency_ms_status_code) AS abs_corr_latency_ms_status_code
    FROM metrics
    WHERE host_stddev_samp_status_code <= 405
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(host) AS host_lower
    FROM filtered
    WHERE host IN ('h4', 'h3', 'h2')
)
SELECT
    host,
    status_code_cume_dist,
    corr_latency_ms_status_code,
    abs_corr_latency_ms_status_code,
    host_lower,
    CASE WHEN host RLIKE '^(h2|h1)$' THEN 'PRIORITY' WHEN host RLIKE '^(h3|h4)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS host_zone
FROM cleaned
ORDER BY host DESC NULLS LAST, status_code_cume_dist;

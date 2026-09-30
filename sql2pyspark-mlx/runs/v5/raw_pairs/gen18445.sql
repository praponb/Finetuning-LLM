WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, value
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        host,
        value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8
    FROM exploded
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        value,
        flag_bit8,
        status_code_high * flag_bit8 AS status_code_high_times_flag_bit8
    FROM flattened_attrs
    WHERE host != 'h1'
),
filtered_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(host) AS clean_host,
        LOWER(host) AS host_lower
    FROM filtered
    WHERE status_code_high_times_flag_bit8 < 58
),
base AS (
    -- Keep recent, valid rows
    SELECT
        host,
        flag_bit8,
        status_code_high_times_flag_bit8,
        clean_host,
        host_lower,
        LEAST(flag_bit8, status_code_high_times_flag_bit8) AS least_flag_bit8_status_code_high_times_flag_bit8
    FROM filtered_app_logs
    WHERE status_code_high_times_flag_bit8 > 72
),
base_app_logs AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(status_code_high_times_flag_bit8, 0) AS status_code_high_times_flag_bit8_filled
    FROM base
    WHERE least_flag_bit8_status_code_high_times_flag_bit8 > 31
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT *, STDDEV_SAMP(least_flag_bit8_status_code_high_times_flag_bit8) OVER (PARTITION BY host) AS host_stddev_samp_least_flag_bit8_status_code_high_times_flag_bit8
    FROM base_app_logs
)
-- Multi-level aggregation
SELECT
    CASE WHEN host_stddev_samp_least_flag_bit8_status_code_high_times_flag_bit8 >= 73 THEN 'PREMIUM' ELSE 'STANDARD' END AS host_stddev_samp_least_flag_bit8_status_code_high_times_flag_bit8_tier,
    host,
    COUNT_IF(host = 'h1') AS h1_events,
    SORT_ARRAY(COLLECT_SET(host)) AS host_list,
    PERCENTILE_APPROX(least_flag_bit8_status_code_high_times_flag_bit8, 0.9) AS least_flag_bit8_status_code_high_times_flag_bit8_p90
FROM metrics
GROUP BY CUBE (CASE WHEN host_stddev_samp_least_flag_bit8_status_code_high_times_flag_bit8 >= 73 THEN 'PREMIUM' ELSE 'STANDARD' END, host)
ORDER BY host_stddev_samp_least_flag_bit8_status_code_high_times_flag_bit8_tier DESC

WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, attr_value
),
unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        latency_ms,
        status_code,
        key,
        CAST((status_code & 4) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
    WHERE key IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        status_code,
        key,
        STDDEV_SAMP(status_code_high) OVER w_running AS key_stddev_samp_status_code_high,
        SUM(status_code_high) OVER w_running AS key_sum_status_code_high
    FROM unpacked
    WHERE key IN ('trace', 'version')
    WINDOW w_running AS (PARTITION BY key)
),
enriched_app_logs AS (
    -- Running and moving aggregates
    SELECT
        key,
        key_stddev_samp_status_code_high,
        AVG(key_sum_status_code_high) OVER w_hist AS key_avg_key_sum_status_code_high,
        AVG(key_sum_status_code_high) OVER w_hist AS key_avg_key_sum_status_code_high_2
    FROM enriched
    WINDOW w_hist AS (PARTITION BY key)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(key_avg_key_sum_status_code_high_2, 0) AS key_avg_key_sum_status_code_high_2_filled
    FROM enriched_app_logs
    WHERE key IS NOT NULL
)
SELECT
    key_stddev_samp_status_code_high,
    key_avg_key_sum_status_code_high,
    key_avg_key_sum_status_code_high_2,
    key_avg_key_sum_status_code_high_2_filled
FROM filtered

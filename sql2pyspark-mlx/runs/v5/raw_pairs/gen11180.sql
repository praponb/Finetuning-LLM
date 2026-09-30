WITH flattened AS (
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
),
parsed AS (
    SELECT
        id,
        latency_ms,
        status_code,
        value,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE status_code < 437
),
cleaned AS (
    SELECT
        id,
        latency_ms,
        status_code,
        flag_bit8,
        status_code_high,
        status_code / NULLIF(flag_bit8, 0) AS status_code_per_flag_bit8,
        CAST(status_code_high AS DOUBLE) AS status_code_high_num
    FROM parsed
    WHERE flag_bit8 < 1
),
valid AS (
    SELECT
        id,
        latency_ms,
        status_code,
        status_code_high,
        status_code_per_flag_bit8,
        status_code_high_num,
        CEIL(latency_ms) AS latency_ms_whole,
        LEAST(status_code, status_code_per_flag_bit8) AS least_status_code_status_code_per_flag_bit8
    FROM cleaned
    WHERE status_code_high >= 2
),
valid_app_logs AS (
    SELECT
        latency_ms,
        status_code,
        status_code_per_flag_bit8,
        status_code_high_num,
        latency_ms_whole,
        least_status_code_status_code_per_flag_bit8,
        status_code_high - latency_ms AS status_code_high_minus_latency_ms
    FROM valid
    WHERE latency_ms = 1232
),
base AS (
    SELECT
        *,
        ROUND(latency_ms * 100, 2) AS latency_ms_adjusted,
        least_status_code_status_code_per_flag_bit8 + latency_ms_whole AS least_status_code_status_code_per_flag_bit8_plus_latency_ms_whole
    FROM valid_app_logs
    WHERE latency_ms BETWEEN 1291 AND 1879
),
cleaned_app_logs AS (
    SELECT
        status_code,
        status_code_per_flag_bit8,
        status_code_high_num,
        least_status_code_status_code_per_flag_bit8,
        status_code_high_minus_latency_ms,
        latency_ms_adjusted,
        least_status_code_status_code_per_flag_bit8_plus_latency_ms_whole,
        CEIL(latency_ms_whole) AS latency_ms_whole_whole,
        latency_ms * status_code_high_num AS latency_ms_times_status_code_high_num
    FROM base
    WHERE least_status_code_status_code_per_flag_bit8_plus_latency_ms_whole >= 79
),
stats AS (
    SELECT
        CASE WHEN latency_ms_adjusted >= 65 THEN 'HIGH' ELSE 'LOW' END AS latency_ms_adjusted_tier,
        CASE WHEN status_code_high_num >= 74 THEN 'LARGE' ELSE 'SMALL' END AS status_code_high_num_tier,
        BIT_OR(status_code) AS status_code_bit_or,
        PERCENTILE_APPROX(latency_ms_times_status_code_high_num, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_times_status_code_high_num_quartiles
    FROM cleaned_app_logs
    GROUP BY CASE WHEN latency_ms_adjusted >= 65 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN status_code_high_num >= 74 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    latency_ms_adjusted_tier,
    status_code_high_num_tier,
    status_code_bit_or,
    latency_ms_times_status_code_high_num_quartiles
FROM stats
ORDER BY status_code_bit_or

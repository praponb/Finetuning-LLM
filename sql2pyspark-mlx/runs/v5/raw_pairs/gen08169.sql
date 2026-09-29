WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, attr_value
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        latency_ms,
        status_code,
        attr_value,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(flag_bit4 * 0.9, 2) AS flag_bit4_adjusted
    FROM flattened_attrs
    WHERE flag_bit4 <= 0
),
filtered_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(status_code AS DOUBLE) AS status_code_str,
        flag_bit4 * status_code_high AS flag_bit4_times_status_code_high
    FROM filtered
    WHERE latency_ms BETWEEN 1040 AND 1648
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_code_str >= 79 THEN 'HIGH' ELSE 'LOW' END AS status_code_str_tier,
    CASE WHEN flag_bit4 >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS flag_bit4_tier,
    CASE WHEN flag_bit4_times_status_code_high >= 80 THEN 'HIGH' ELSE 'LOW' END AS flag_bit4_times_status_code_high_tier,
    SUM(flag_bit4_adjusted) AS total_flag_bit4_adjusted,
    ROUND(AVG(flag_bit4), 2) AS avg_flag_bit4,
    SUM(status_code_str) AS total_status_code_str,
    MIN(status_code) AS min_status_code,
    PERCENTILE_APPROX(flag_bit4_times_status_code_high, 0.9) AS flag_bit4_times_status_code_high_p90,
    PERCENTILE_APPROX(flag_bit4_adjusted, ARRAY(0.25, 0.5, 0.75)) AS flag_bit4_adjusted_quartiles
FROM filtered_app_logs
GROUP BY CUBE (CASE WHEN status_code_str >= 79 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN flag_bit4 >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN flag_bit4_times_status_code_high >= 80 THEN 'HIGH' ELSE 'LOW' END);

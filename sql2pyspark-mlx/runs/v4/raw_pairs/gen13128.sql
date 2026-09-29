WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        temp_c - status_bits AS temp_c_minus_status_bits,
        INITCAP(TRIM(firmware)) AS firmware_title
    FROM device_readings
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 12:00:00')
      AND device_code IN ('DV-02', 'DV-04')
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(temp_c) OVER (ORDER BY is_new_burst, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_temp_c,
        SUM(temp_c) OVER (PARTITION BY device_code ORDER BY session_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_temp_c,
        SUM(humidity) OVER w_part AS humidity_sum_15m
    FROM sessions_device_readings
    WINDOW w_part AS (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
),
valid_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, humidity_sum_15m + temp_c AS humidity_sum_15m_plus_temp_c
    FROM enriched
    WHERE humidity >= 48
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS row_num
        FROM valid_device_readings
    ) ranked
    WHERE row_num = 1
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN temp_c_minus_status_bits >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END AS temp_c_minus_status_bits_tier,
        device_code,
        SUM(CASE WHEN device_code = 'DV-01' THEN running_temp_c ELSE 0 END) AS dv_01_running_temp_c,
        MIN(reading_ts) AS first_seen,
        SUM(CASE WHEN device_code = 'DV-04' THEN humidity_sum_15m_plus_temp_c ELSE 0 END) AS dv_04_humidity_sum_15m_plus_temp_c,
        ROUND(SUM(humidity) / NULLIF(COUNT(CASE WHEN device_code = 'DV-03' THEN 1 END), 0), 2) AS humidity_per_dv_03,
        PERCENTILE_APPROX(humidity, 0.95) AS humidity_p95,
        GROUPING(CASE WHEN temp_c_minus_status_bits >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
    FROM ranked
    GROUP BY ROLLUP (CASE WHEN temp_c_minus_status_bits >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END, device_code)
)
SELECT
    temp_c_minus_status_bits_tier,
    first_seen,
    dv_04_humidity_sum_15m_plus_temp_c,
    grouping_level
FROM stats
ORDER BY grouping_level DESC;

WITH matched_keys AS (
    -- Set difference on keys
    SELECT firmware, id
    FROM device_readings
    EXCEPT DISTINCT
    SELECT firmware, id
    FROM device_readings
    WHERE TO_DATE(reading_ts) < '2024-01-11'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM device_readings s
    JOIN matched_keys k ON s.firmware = k.firmware AND s.id = k.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        CORR(temp_c, humidity) OVER w_part AS corr_temp_c_humidity,
        COUNT(temp_c) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS temp_c_count_30m,
        COUNT(pressure) OVER w_part AS device_code_count_pressure,
        CORR(status_bits, humidity) OVER w_part AS corr_status_bits_humidity,
        CORR(temp_c, humidity) OVER w_part AS corr_temp_c_humidity_2
    FROM verified
    WINDOW w_part AS (PARTITION BY device_code)
),
with_windows_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(device_code_count_pressure) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS device_code_count_pressure_sum_2h,
        MIN(corr_status_bits_humidity) OVER (PARTITION BY device_code) AS device_code_min_corr_status_bits_humidity,
        RANK() OVER (PARTITION BY device_code ORDER BY pressure DESC NULLS FIRST) AS pressure_rank,
        SUM(status_bits) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_bits_sum_1h,
        CUME_DIST() OVER (ORDER BY corr_temp_c_humidity_2 DESC) AS corr_temp_c_humidity_2_cume_dist
    FROM with_windows
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, device_code_count_pressure - device_code_count_pressure_sum_2h AS device_code_count_pressure_minus_device_code_count_pressure_sum_2h
    FROM with_windows_device_readings
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 16:00:00')
      AND temp_c_count_30m BETWEEN 9 AND 14
),
valid_device_readings AS (
    -- Basic cleansing and defaults
    SELECT *, device_code_min_corr_status_bits_humidity - temp_c_count_30m AS device_code_min_corr_status_bits_humidity_minus_temp_c_count_30m
    FROM valid
    WHERE reading_ts >= TIMESTAMP '2025-09-01 15:00:00'
      AND device_code = 'DV-01'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY corr_status_bits_humidity ASC NULLS LAST) AS corr_status_bits_humidity_dense_rank,
        ROUND(pressure / SUM(pressure) OVER (PARTITION BY device_code), 4) AS pressure_share,
        COUNT(device_code_count_pressure_minus_device_code_count_pressure_sum_2h) OVER (ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS device_code_count_pressure_minus_device_code_count_pressure_sum_2h_count_2h,
        SUM(device_code_min_corr_status_bits_humidity_minus_temp_c_count_30m) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS device_code_min_corr_status_bits_humidity_minus_temp_c_count_30m_sum_2h
    FROM valid_device_readings
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        corr_temp_c_humidity,
        temp_c_count_30m,
        device_code_count_pressure,
        corr_status_bits_humidity,
        corr_temp_c_humidity_2,
        device_code_count_pressure_sum_2h,
        corr_temp_c_humidity_2_cume_dist,
        device_code_count_pressure_minus_device_code_count_pressure_sum_2h,
        device_code_min_corr_status_bits_humidity_minus_temp_c_count_30m,
        pressure_share,
        device_code_count_pressure_minus_device_code_count_pressure_sum_2h_count_2h,
        device_code_min_corr_status_bits_humidity_minus_temp_c_count_30m_sum_2h,
        corr_status_bits_humidity * temp_c_count_30m AS corr_status_bits_humidity_times_temp_c_count_30m
    FROM enriched
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 16:00:00')
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        device_code,
        DATE_TRUNC('HOUR', reading_ts) AS hour_window,
        CASE WHEN corr_temp_c_humidity_2 >= 0.52 THEN 'HIGH' ELSE 'LOW' END AS corr_temp_c_humidity_2_tier,
        MIN(reading_ts) AS first_seen,
        COUNT_IF(device_code = 'DV-02') AS dv_02_events,
        ROUND(AVG(device_code_count_pressure_sum_2h), 2) AS avg_device_code_count_pressure_sum_2h,
        SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
        ROUND(AVG(corr_status_bits_humidity_times_temp_c_count_30m), 2) AS avg_corr_status_bits_humidity_times_temp_c_count_30m,
        GROUPING_ID() AS gid
    FROM base
    GROUP BY GROUPING SETS ((device_code, DATE_TRUNC('HOUR', reading_ts), CASE WHEN corr_temp_c_humidity_2 >= 0.52 THEN 'HIGH' ELSE 'LOW' END), (device_code, DATE_TRUNC('HOUR', reading_ts)), ())
)
SELECT
    hour_window,
    corr_temp_c_humidity_2_tier,
    first_seen,
    dv_02_events,
    avg_device_code_count_pressure_sum_2h,
    device_code_list,
    avg_corr_status_bits_humidity_times_temp_c_count_30m
FROM summary;

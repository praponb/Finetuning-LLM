WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        temp_c * humidity AS temp_c_times_humidity,
        temp_c * humidity AS temp_c_times_humidity_2
    FROM device_readings
    WHERE humidity < 71
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(temp_c_times_humidity) OVER (PARTITION BY device_code ORDER BY humidity, id) AS next_temp_c_times_humidity,
        LAG(temp_c) OVER (PARTITION BY device_code ORDER BY humidity, id) AS prev_temp_c,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity, id) AS seq_no
    FROM base
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        status_bits,
        temp_c_times_humidity_2,
        prev_temp_c,
        seq_no,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c DESC, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY next_temp_c_times_humidity, id) AS event_seq_2,
        LAG(next_temp_c_times_humidity, 1) OVER (PARTITION BY device_code ORDER BY temp_c_times_humidity, id) AS prev_next_temp_c_times_humidity,
        LAG(status_bits, 1) OVER (PARTITION BY device_code ORDER BY seq_no, id) AS prev_status_bits
    FROM scored
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        status_bits,
        temp_c_times_humidity_2,
        prev_temp_c,
        event_seq_2,
        prev_next_temp_c_times_humidity,
        prev_status_bits,
        CAST(prev_status_bits AS STRING) AS prev_status_bits_num
    FROM with_windows
    WHERE device_code = 'DV-03'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(status_bits, 2) AS status_bits_rounded
    FROM filtered
    WHERE event_seq_2 <= 8
),
cleaned_device_readings AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        prev_status_bits * prev_next_temp_c_times_humidity AS prev_status_bits_times_prev_next_temp_c_times_humidity,
        ROUND(status_bits_rounded * 1.1, 2) AS adjusted_status_bits_rounded
    FROM cleaned
    WHERE status_bits >= 9
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(adjusted_status_bits_rounded) OVER (ORDER BY status_bits_rounded, id) AS prev_adjusted_status_bits_rounded,
        LEAD(adjusted_status_bits_rounded) OVER (ORDER BY status_bits_rounded, id) AS next_adjusted_status_bits_rounded,
        LEAD(status_bits) OVER (ORDER BY status_bits_rounded, id) AS next_status_bits,
        ROW_NUMBER() OVER (ORDER BY status_bits_rounded, id) AS row_seq,
        CUME_DIST() OVER (ORDER BY prev_status_bits NULLS LAST) AS prev_status_bits_cume_dist
    FROM cleaned_device_readings
    WHERE status_bits < 4
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
        CASE WHEN prev_temp_c >= 34.1 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_temp_c_tier,
        CASE WHEN prev_next_temp_c_times_humidity >= 68 THEN 'LARGE' ELSE 'SMALL' END AS prev_next_temp_c_times_humidity_tier,
        APPROX_PERCENTILE(prev_status_bits, ARRAY(0.25, 0.5, 0.75)) AS prev_status_bits_quartiles,
        ROUND(AVG(adjusted_status_bits_rounded), 2) AS avg_adjusted_status_bits_rounded,
        APPROX_PERCENTILE(adjusted_status_bits_rounded, ARRAY(0.25, 0.5, 0.75)) AS adjusted_status_bits_rounded_quartiles,
        GROUPING(CASE WHEN row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END) AS gid
    FROM metrics
    GROUP BY GROUPING SETS ((CASE WHEN row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN prev_temp_c >= 34.1 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN prev_next_temp_c_times_humidity >= 68 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN prev_temp_c >= 34.1 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END))
)
SELECT
    row_seq_tier,
    prev_temp_c_tier,
    prev_next_temp_c_times_humidity_tier,
    prev_status_bits_quartiles,
    avg_adjusted_status_bits_rounded,
    gid
FROM summary
ORDER BY row_seq_tier NULLS FIRST

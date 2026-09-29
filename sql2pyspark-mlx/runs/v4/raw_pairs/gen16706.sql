WITH valid AS (
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        status_bits - pressure AS status_bits_minus_pressure
    FROM device_readings
    WHERE reading_ts > TO_DATE('2025-09-01')
      AND reading_ts >= '2024-12-01'
),
scored AS (
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        status_bits_minus_pressure,
        NTILE(3) OVER (PARTITION BY device_code ORDER BY humidity DESC, id) AS humidity_bucket,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c DESC, id) AS seq_no,
        CORR(status_bits, temp_c) OVER (PARTITION BY device_code) AS corr_status_bits_temp_c
    FROM valid
),
valid_device_readings AS (
    SELECT *, GREATEST(status_bits_minus_pressure, pressure) AS greatest_status_bits_minus_pressure_pressure
    FROM scored
    WHERE pressure <= 1008
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY status_bits_minus_pressure DESC) AS row_num
    FROM valid_device_readings
    QUALIFY row_num = 1
),
ranked_device_readings AS (
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity, id) = 1
)
SELECT
    device_code,
    seq_no,
    STDDEV_SAMP(status_bits) AS status_bits_stddev,
    ROUND(AVG(pressure), 2) AS avg_pressure,
    SUM(CASE WHEN device_code = 'DV-01' THEN pressure ELSE 0 END) AS dv_01_pressure,
    BIT_OR(status_bits) AS status_bits_bit_or,
    PERCENTILE_APPROX(humidity_bucket, 0.9) AS humidity_bucket_p90,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list
FROM ranked_device_readings
GROUP BY CUBE (device_code, seq_no)
ORDER BY dv_01_pressure NULLS LAST;

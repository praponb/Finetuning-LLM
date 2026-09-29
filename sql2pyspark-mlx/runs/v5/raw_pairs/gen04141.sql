WITH base AS (
    SELECT
        *,
        LOWER(device_code) AS device_code_lower,
        CONCAT(firmware, ', ', device_code) AS firmware_device_code
    FROM device_readings
    WHERE firmware = '1.1.0'
),
prev_events AS (
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM base
),
gaps AS (
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
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
        firmware_device_code,
        session_start_flag,
        burst_id,
        CORR(temp_c, pressure) OVER (PARTITION BY device_code) AS corr_temp_c_pressure,
        NTILE(10) OVER (ORDER BY status_bits, id) AS status_bits_bucket
    FROM islands
),
remaining_keys AS (
    SELECT device_code, firmware
    FROM scored
    EXCEPT DISTINCT
    SELECT device_code, firmware
    FROM device_readings
    WHERE device_code = 'DV-02'
),
kept_rows AS (
    SELECT s.*
    FROM scored s
    JOIN remaining_keys k ON s.device_code = k.device_code AND s.firmware = k.firmware
)
SELECT
    CASE WHEN status_bits >= 13 THEN 'LARGE' ELSE 'SMALL' END AS status_bits_tier,
    burst_id,
    PERCENTILE_APPROX(pressure, ARRAY(0.25, 0.5, 0.75)) AS pressure_quartiles,
    BIT_OR(status_bits) AS status_bits_bit_or,
    APPROX_PERCENTILE(temp_c, ARRAY(0.25, 0.5, 0.75)) AS temp_c_quartiles,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    ROUND(SUM(corr_temp_c_pressure) / NULLIF(COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END), 0), 2) AS corr_temp_c_pressure_per_dv_01,
    GROUPING_ID() AS grouping_level
FROM kept_rows
GROUP BY GROUPING SETS ((CASE WHEN status_bits >= 13 THEN 'LARGE' ELSE 'SMALL' END, burst_id), (CASE WHEN status_bits >= 13 THEN 'LARGE' ELSE 'SMALL' END))
HAVING ROUND(SUM(corr_temp_c_pressure) / NULLIF(COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END), 0), 2) > 0
ORDER BY burst_id DESC;

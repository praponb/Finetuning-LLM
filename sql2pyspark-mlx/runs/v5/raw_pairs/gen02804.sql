WITH filtered AS (
    SELECT *, pressure - status_bits AS pressure_minus_status_bits
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
with_windows AS (
    SELECT
        id,
        device_code,
        reading_ts,
        pressure,
        pressure_minus_status_bits,
        AVG(temp_c) OVER (PARTITION BY device_code ORDER BY humidity ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS temp_c_avg_5,
        LAG(status_bits, 1) OVER (ORDER BY humidity ASC, id ASC) AS prev_status_bits,
        LAST_VALUE(temp_c) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_temp_c
    FROM filtered
),
filtered_device_readings AS (
    SELECT *, prev_status_bits + pressure_minus_status_bits AS prev_status_bits_plus_pressure_minus_status_bits
    FROM with_windows
    WHERE reading_ts > TIMESTAMP '2025-09-01 04:00:00'
),
remaining_keys AS (
    SELECT id, device_code
    FROM filtered_device_readings
    EXCEPT ALL
    SELECT id, device_code
    FROM device_readings
    WHERE firmware IN ('1.0.3', '2.0.1', '1.1.0')
),
reconciled AS (
    SELECT s.*
    FROM filtered_device_readings s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.device_code = k.device_code
)
SELECT
    device_code,
    reading_ts,
    pressure,
    pressure_minus_status_bits,
    temp_c_avg_5,
    prev_status_bits,
    prev_status_bits_plus_pressure_minus_status_bits
FROM reconciled
ORDER BY prev_status_bits_plus_pressure_minus_status_bits DESC NULLS FIRST

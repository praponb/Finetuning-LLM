WITH base AS (
    SELECT
        id,
        device_code,
        reading_ts,
        pressure,
        firmware,
        humidity / NULLIF(status_bits, 0) AS humidity_per_status_bits
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
valid AS (
    SELECT *, UPPER(device_code) AS device_code_upper
    FROM base
    WHERE reading_ts >= TIMESTAMP '2025-09-01 09:00:00'
      AND device_code NOT IN ('DV-02', 'DV-01', 'DV-04')
),
cleaned AS (
    SELECT *, LENGTH(firmware) AS firmware_length
    FROM valid
    WHERE TO_DATE(reading_ts) < '2025-08-01'
),
gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned
),
sessions_device_readings AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
matched_keys AS (
    SELECT device_code, firmware
    FROM sessions_device_readings
    INTERSECT
    SELECT device_code, firmware
    FROM device_readings
    WHERE device_code = 'DV-04'
),
verified AS (
    SELECT s.*
    FROM sessions_device_readings s
    INNER JOIN matched_keys k ON s.device_code = k.device_code AND s.firmware = k.firmware
)
SELECT
    device_code,
    reading_ts,
    pressure,
    firmware,
    humidity_per_status_bits,
    device_code_upper,
    session_id,
    CASE WHEN device_code RLIKE '^(DV-01|DV-04)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS device_code_zone
FROM verified

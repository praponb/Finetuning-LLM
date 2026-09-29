WITH kept_keys AS (
    SELECT device_code, id
    FROM device_readings
    EXCEPT ALL
    SELECT device_code, id
    FROM device_readings
    WHERE device_code = 'DV-02'
),
kept_rows AS (
    SELECT s.*
    FROM device_readings s
    INNER JOIN kept_keys k ON s.device_code = k.device_code AND s.id = k.id
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    status_bits,
    firmware
FROM kept_rows
ORDER BY firmware DESC, temp_c DESC

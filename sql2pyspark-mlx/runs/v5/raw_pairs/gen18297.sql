WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS rn
    FROM device_readings
    QUALIFY rn = 1
)
SELECT
    id,
    temp_c,
    humidity,
    pressure,
    firmware,
    rn
FROM ranked;

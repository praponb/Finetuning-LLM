SELECT
    id,
    reading_ts,
    pressure,
    status_bits,
    firmware
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits, id) AS rn
    FROM device_readings
) ranked
WHERE rn = 1;

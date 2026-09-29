WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits, id) AS row_num
        FROM device_readings
    ) x
    WHERE row_num = 1
)
SELECT
    id,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    firmware,
    row_num
FROM deduped
ORDER BY temp_c NULLS FIRST

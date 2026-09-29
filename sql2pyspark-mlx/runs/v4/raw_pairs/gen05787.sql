WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        TRIM(device_code) AS clean_device_code
    FROM device_readings
    WHERE TO_DATE(reading_ts) >= '2024-11-01'
)
-- Deduplicate with ROW_NUMBER
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure, id) AS dedup_rank
    FROM base
) ranked
WHERE dedup_rank = 1

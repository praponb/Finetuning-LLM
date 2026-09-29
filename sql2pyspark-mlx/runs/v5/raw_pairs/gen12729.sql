WITH base AS (
    SELECT *, INITCAP(TRIM(firmware)) AS firmware_title
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    pressure,
    firmware_title
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts DESC, id) AS rank_in_group
    FROM base
) t
WHERE rank_in_group = 1;

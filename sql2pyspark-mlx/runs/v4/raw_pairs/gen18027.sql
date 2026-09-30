WITH latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC, id) AS dedup_rank
        FROM device_readings
    ) x
    WHERE dedup_rank = 1
)
SELECT
    id,
    device_code,
    temp_c,
    pressure,
    firmware
FROM latest
ORDER BY pressure DESC NULLS LAST, temp_c DESC;

WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY reading_ts DESC) AS rank_in_group
    FROM device_readings
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    reading_ts,
    temp_c,
    pressure,
    status_bits,
    firmware
FROM latest
ORDER BY id DESC NULLS FIRST;

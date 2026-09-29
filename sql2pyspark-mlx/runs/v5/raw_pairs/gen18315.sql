WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        COALESCE(pressure, 0) pressure_filled
    FROM device_readings
    WHERE reading_ts < '2024-04-01'
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, LEAST(humidity, temp_c) least_humidity_temp_c
    FROM valid
    WHERE status_bits < 8
),
remaining_keys AS (
    -- Set difference on keys
    SELECT firmware, id
    FROM base
    EXCEPT DISTINCT
    SELECT firmware, id
    FROM device_readings
    WHERE pressure <= 1025
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.firmware = k.firmware AND s.id = k.id
),
valid_device_readings AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        firmware,
        pressure_filled,
        ROUND(pressure, 1) pressure_rounded,
        ROUND(pressure_filled / humidity, 2) pressure_filled_per_humidity
    FROM verified
    WHERE reading_ts > TIMESTAMP '2025-09-01 08:00:00'
),
valid_device_readings_2 AS (
    -- Keep recent, valid rows
    SELECT
        id,
        reading_ts,
        humidity,
        firmware,
        pressure_filled,
        pressure_rounded,
        pressure_filled_per_humidity,
        ROUND(temp_c * 0.85, 2) temp_c_scaled
    FROM valid_device_readings
    WHERE reading_ts > DATE '2025-09-01'
      AND humidity > 36
)
SELECT
    humidity,
    firmware,
    pressure_rounded,
    pressure_filled_per_humidity,
    temp_c_scaled
FROM valid_device_readings_2
ORDER BY firmware DESC NULLS FIRST;

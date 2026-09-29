WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        CASE WHEN humidity >= 81 THEN 'High' WHEN humidity >= 36 THEN 'Medium' ELSE 'Low' END AS humidity_level
    FROM device_readings
    WHERE TO_DATE(reading_ts) >= '2024-01-01'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        temp_c,
        pressure,
        firmware
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id ASC) = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN firmware IN ('1.0.3', '1.1.0') THEN 'Group A' ELSE 'Other' END AS firmware_group
    FROM latest
    WHERE pressure > 994
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        pressure,
        firmware,
        firmware_group,
        ROUND(temp_c * 0.9, 2) AS temp_c_scaled,
        ROUND(temp_c / pressure, 2) AS temp_c_per_pressure
    FROM valid
    WHERE temp_c >= 25
)
SELECT
    id,
    firmware,
    firmware_group,
    temp_c_scaled,
    temp_c_per_pressure
FROM cleaned
ORDER BY id DESC NULLS FIRST
LIMIT 10

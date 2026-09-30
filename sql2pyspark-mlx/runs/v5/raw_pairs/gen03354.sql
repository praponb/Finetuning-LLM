WITH ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure NULLS LAST, id) AS rank_in_group
        FROM device_readings
    ) x
    WHERE rank_in_group = 1
),
remaining_keys AS (
    SELECT id, device_code
    FROM ranked
    INTERSECT ALL
    SELECT id, device_code
    FROM device_readings
    WHERE HOUR(reading_ts) BETWEEN 8 AND 14
),
verified AS (
    SELECT s.*
    FROM ranked s
    JOIN remaining_keys k ON s.id = k.id AND s.device_code = k.device_code
),
remaining_keys_2 AS (
    SELECT firmware, device_code
    FROM verified
    EXCEPT DISTINCT
    SELECT firmware, device_code
    FROM device_readings
    WHERE firmware IN ('1.0.3', '1.1.0', '2.0.1')
),
verified_device_readings AS (
    SELECT s.*
    FROM verified s
    JOIN remaining_keys_2 k ON s.firmware = k.firmware AND s.device_code = k.device_code
),
base AS (
    SELECT *, ROUND(humidity, 1) AS humidity_rounded
    FROM verified_device_readings
    WHERE pressure < 1016
),
with_windows AS (
    SELECT
        *,
        SUM(humidity_rounded) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS humidity_rounded_sum_30m,
        MAX(humidity_rounded) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS humidity_rounded_max_2h,
        COUNT(pressure) OVER w_part AS pressure_count_1h
    FROM base
    WINDOW w_part AS (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
valid AS (
    SELECT
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        firmware,
        humidity_rounded,
        humidity_rounded_sum_30m,
        humidity_rounded_max_2h,
        pressure_count_1h,
        pressure_count_1h - humidity_rounded_max_2h AS pressure_count_1h_minus_humidity_rounded_max_2h
    FROM with_windows
    WHERE HOUR(reading_ts) BETWEEN 11 AND 17
),
base_device_readings AS (
    SELECT
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        humidity_rounded,
        humidity_rounded_sum_30m,
        pressure_count_1h,
        pressure_count_1h_minus_humidity_rounded_max_2h,
        FLOOR(status_bits) AS status_bits_whole
    FROM valid
    WHERE firmware = '2.0.1'
)
SELECT
    reading_ts,
    temp_c,
    humidity,
    humidity_rounded,
    humidity_rounded_sum_30m,
    pressure_count_1h,
    status_bits_whole
FROM base_device_readings
ORDER BY pressure_count_1h DESC;

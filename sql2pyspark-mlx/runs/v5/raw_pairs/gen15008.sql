WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(temp_c AS INT) AS temp_c_int,
        status_bits * humidity AS status_bits_times_humidity
    FROM device_readings
    WHERE pressure BETWEEN 996 AND 1023
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY temp_c_int DESC NULLS FIRST) AS row_num
        FROM valid
    ) t
    WHERE row_num = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(status_bits) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_bits_max_15m,
        SUM(temp_c) OVER (PARTITION BY device_code ORDER BY status_bits_times_humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_temp_c,
        SUM(temp_c) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS temp_c_sum_2h
    FROM deduped
    WHERE pressure BETWEEN 1010 AND 1027
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c_sum_2h DESC, id) AS rn
    FROM with_windows
    QUALIFY rn = 1
)
SELECT
    id,
    reading_ts,
    temp_c,
    pressure,
    status_bits,
    firmware,
    temp_c_int,
    status_bits_times_humidity,
    row_num
FROM latest

WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LEAST(pressure, status_bits) least_pressure_status_bits,
        temp_c * humidity temp_c_times_humidity
    FROM device_readings
    WHERE status_bits BETWEEN 6 AND 10
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM valid
    QUALIFY RANK() OVER (PARTITION BY device_code ORDER BY status_bits DESC) = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(status_bits) OVER w_part prev_status_bits,
        LEAD(status_bits) OVER w_part next_status_bits,
        LEAD(temp_c_times_humidity) OVER w_part next_temp_c_times_humidity,
        LAG(temp_c_times_humidity) OVER w_part prev_temp_c_times_humidity,
        ROW_NUMBER() OVER w_part event_seq,
        FIRST_VALUE(least_pressure_status_bits) IGNORE NULLS OVER (ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_least_pressure_status_bits
    FROM deduped
    WINDOW w_part AS (PARTITION BY device_code ORDER BY temp_c_times_humidity, id)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        device_code,
        pressure,
        status_bits,
        least_pressure_status_bits,
        temp_c_times_humidity,
        next_temp_c_times_humidity,
        event_seq,
        first_known_least_pressure_status_bits
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC NULLS FIRST, id) AS latest_rank
        FROM with_windows
    ) x
    WHERE latest_rank = 1
),
with_windows_device_readings AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        status_bits,
        temp_c_times_humidity,
        next_temp_c_times_humidity,
        LAG(status_bits) OVER (PARTITION BY device_code ORDER BY temp_c_times_humidity, id) prev_status_bits,
        LEAD(status_bits) OVER (PARTITION BY device_code ORDER BY temp_c_times_humidity, id) next_status_bits,
        LEAD(pressure) OVER (PARTITION BY device_code ORDER BY temp_c_times_humidity, id) next_pressure,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c_times_humidity, id) event_seq_2,
        CORR(pressure, status_bits) OVER (PARTITION BY device_code) corr_pressure_status_bits
    FROM ranked
),
deduped_device_readings AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY next_status_bits DESC NULLS LAST) AS rn
        FROM with_windows_device_readings
    ) ranked
    WHERE rn = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(prev_status_bits) OVER (PARTITION BY device_code ORDER BY event_seq_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) prev_status_bits_avg_6,
        AVG(corr_pressure_status_bits) OVER (PARTITION BY device_code ORDER BY prev_status_bits, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) corr_pressure_status_bits_avg_5,
        NTILE(5) OVER (PARTITION BY device_code ORDER BY temp_c_times_humidity DESC, id) temp_c_times_humidity_bucket
    FROM deduped_device_readings
    WHERE corr_pressure_status_bits <= 0.2
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        device_code,
        next_pressure,
        corr_pressure_status_bits,
        temp_c_times_humidity,
        next_temp_c_times_humidity
    FROM scored
) src
UNPIVOT INCLUDE NULLS (
    val FOR metric_name IN (next_pressure AS NEXT_PRESSURE_VAL, corr_pressure_status_bits AS CORR_PRESSURE_STATUS_BITS, temp_c_times_humidity, next_temp_c_times_humidity AS NEXT_TEMP_C_TIMES_HUMIDITY_VAL)
)

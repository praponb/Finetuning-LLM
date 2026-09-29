WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS FIRST, id) AS rn
        FROM device_readings
    ) x
    WHERE rn = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT firmware
    FROM deduped
    INTERSECT ALL
    SELECT firmware
    FROM device_readings
    WHERE device_code IN ('DV-02', 'DV-04', 'DV-01')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN kept_keys k ON s.firmware = k.firmware
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(device_code, ', ', firmware) AS device_code_firmware
    FROM kept_rows
    WHERE status_bits > 5
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        rn,
        device_code_firmware,
        DENSE_RANK() OVER (ORDER BY humidity DESC) AS humidity_dense_rank,
        RANK() OVER w_hist AS pressure_rank,
        SUM(pressure) OVER (PARTITION BY device_code) AS device_code_sum_pressure
    FROM valid
    WINDOW w_hist AS (ORDER BY pressure DESC NULLS FIRST)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(device_code_sum_pressure / pressure_rank, 2) AS device_code_sum_pressure_per_pressure_rank
    FROM scored
    WHERE reading_ts > TO_DATE('2025-09-01')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CORR(status_bits, temp_c) OVER w_grp AS corr_status_bits_temp_c,
        PERCENT_RANK() OVER (ORDER BY temp_c DESC NULLS FIRST) AS temp_c_percent_rank,
        MAX(device_code_sum_pressure) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS device_code_sum_pressure_max_15m
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY device_code)
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    firmware,
    rn,
    device_code_firmware
FROM with_windows
ORDER BY temp_c DESC;

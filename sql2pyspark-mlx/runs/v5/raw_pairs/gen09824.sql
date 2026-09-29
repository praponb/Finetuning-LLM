WITH scored AS (
    SELECT
        *,
        LAG(temp_c, 1) OVER (PARTITION BY device_code ORDER BY status_bits ASC, id ASC) prev_temp_c,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity ASC, id ASC) row_seq,
        RANK() OVER (ORDER BY humidity DESC NULLS FIRST) humidity_rank,
        FIRST_VALUE(temp_c, TRUE) OVER (ORDER BY temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_temp_c,
        LAST_VALUE(temp_c) IGNORE NULLS OVER (ORDER BY pressure ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_temp_c
    FROM device_readings
)
SELECT
    id,
    device_code,
    reading_ts,
    humidity,
    pressure,
    status_bits,
    firmware,
    prev_temp_c,
    last_known_temp_c
FROM scored
ORDER BY humidity ASC

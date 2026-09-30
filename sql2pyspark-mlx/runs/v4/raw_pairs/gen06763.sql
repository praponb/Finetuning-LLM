WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(status_bits) status_bits_whole
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 10:00:00'
      AND humidity < 38
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
        status_bits_whole,
        CUME_DIST() OVER (PARTITION BY device_code ORDER BY temp_c ASC NULLS LAST) temp_c_cume_dist,
        SUM(pressure) OVER (PARTITION BY device_code ORDER BY status_bits_whole ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_pressure,
        CORR(humidity, pressure) OVER (PARTITION BY device_code) corr_humidity_pressure
    FROM cleaned
),
scored_device_readings AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(corr_humidity_pressure) OVER w_ordered cumulative_corr_humidity_pressure,
        FIRST_VALUE(pressure) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY running_pressure ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_pressure,
        SUM(temp_c) OVER w_part temp_c_sum_30m
    FROM scored
    WINDOW w_ordered AS (PARTITION BY device_code ORDER BY corr_humidity_pressure ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM scored_device_readings
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(cumulative_corr_humidity_pressure, TRUE) OVER (ORDER BY status_bits_whole ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_cumulative_corr_humidity_pressure,
        FIRST_VALUE(temp_c_sum_30m) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_temp_c_sum_30m,
        NTILE(3) OVER (PARTITION BY device_code ORDER BY status_bits DESC, id ASC) status_bits_bucket
    FROM sessions_device_readings
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(reading_ts AS DATE) reading_ts_day,
        ABS(last_known_cumulative_corr_humidity_pressure) abs_last_known_cumulative_corr_humidity_pressure
    FROM with_windows
    WHERE reading_ts >= DATE '2025-09-01'
)
SELECT
    id,
    device_code,
    temp_c,
    humidity,
    pressure,
    status_bits,
    firmware,
    status_bits_whole,
    temp_c_cume_dist
FROM filtered
ORDER BY pressure ASC;

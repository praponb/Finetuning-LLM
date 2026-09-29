WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) > INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM device_readings
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM sessionized
    EXCEPT DISTINCT
    SELECT id
    FROM device_readings
    WHERE firmware = '1.1.0'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessionized s
    INNER JOIN remaining_keys k ON s.id = k.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), reading_ts) AS days_since_reading_ts
    FROM kept_rows
    WHERE reading_ts >= TIMESTAMP '2025-09-01 01:00:00'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        CORR(pressure, temp_c) OVER w_ordered AS corr_pressure_temp_c,
        RANK() OVER w_hist AS humidity_rank,
        SUM(days_since_reading_ts) OVER w_ordered AS device_code_sum_days_since_reading_ts,
        DENSE_RANK() OVER (ORDER BY temp_c DESC NULLS LAST) AS temp_c_dense_rank,
        MAX(temp_c) OVER w_ordered AS device_code_max_temp_c
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY device_code), w_hist AS (PARTITION BY device_code ORDER BY humidity ASC)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        reading_ts,
        temp_c,
        pressure,
        session_id,
        days_since_reading_ts,
        corr_pressure_temp_c,
        humidity_rank,
        temp_c_dense_rank,
        FLOOR(temp_c_dense_rank) AS temp_c_dense_rank_whole
    FROM metrics
    WHERE temp_c > 32
),
cleaned_device_readings AS (
    -- Basic cleansing and defaults
    SELECT
        reading_ts,
        temp_c,
        pressure,
        session_id,
        days_since_reading_ts,
        humidity_rank,
        temp_c_dense_rank_whole,
        corr_pressure_temp_c * temp_c AS corr_pressure_temp_c_times_temp_c
    FROM cleaned
    WHERE corr_pressure_temp_c <= 0.2
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        temp_c,
        pressure,
        temp_c_dense_rank_whole,
        corr_pressure_temp_c_times_temp_c,
        LEAST(temp_c, humidity_rank) AS least_temp_c_humidity_rank
    FROM cleaned_device_readings
    WHERE humidity_rank = 3
)
SELECT
    temp_c,
    temp_c_dense_rank_whole,
    corr_pressure_temp_c_times_temp_c,
    least_temp_c_humidity_rank
FROM valid

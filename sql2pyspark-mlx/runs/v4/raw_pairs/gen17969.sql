WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM device_readings
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(temp_c) OVER (PARTITION BY device_code) AS device_code_stddev_samp_temp_c,
        FIRST_VALUE(humidity, TRUE) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_humidity,
        LAST_VALUE(humidity, TRUE) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_humidity,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c DESC, id) AS seq_no
    FROM sessions_device_readings
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY device_code_stddev_samp_temp_c DESC NULLS FIRST) AS device_code_stddev_samp_temp_c_rank,
        LAST_VALUE(device_code_stddev_samp_temp_c) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY last_known_humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_device_code_stddev_samp_temp_c
    FROM metrics
),
metrics_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(device_code_stddev_samp_temp_c / SUM(device_code_stddev_samp_temp_c) OVER (PARTITION BY device_code), 4) AS device_code_stddev_samp_temp_c_share,
        LAG(last_known_device_code_stddev_samp_temp_c, 1) OVER (PARTITION BY device_code ORDER BY is_new_session, id) AS prev_last_known_device_code_stddev_samp_temp_c,
        NTILE(3) OVER w_recent AS temp_c_bucket,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY is_new_session DESC, id) AS row_seq,
        DENSE_RANK() OVER (ORDER BY status_bits DESC) AS status_bits_dense_rank
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY temp_c DESC, id)
),
with_windows_device_readings AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(pressure) OVER w_part AS running_pressure,
        ROUND(status_bits_dense_rank / SUM(status_bits_dense_rank) OVER (PARTITION BY device_code), 4) AS status_bits_dense_rank_share
    FROM metrics_device_readings
    WINDOW w_part AS (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        firmware,
        is_new_session,
        island_id,
        device_code_stddev_samp_temp_c,
        last_known_humidity,
        device_code_stddev_samp_temp_c_rank,
        device_code_stddev_samp_temp_c_share,
        temp_c_bucket,
        status_bits_dense_rank,
        running_pressure,
        status_bits_dense_rank_share,
        TO_DATE(reading_ts) AS reading_ts_date
    FROM with_windows_device_readings
    WHERE running_pressure >= 3970
)
SELECT
    id,
    reading_ts,
    temp_c,
    humidity,
    firmware,
    is_new_session,
    device_code_stddev_samp_temp_c,
    last_known_humidity,
    device_code_stddev_samp_temp_c_rank
FROM cleaned

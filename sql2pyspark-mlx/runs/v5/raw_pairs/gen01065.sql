WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM device_readings
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(pressure) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_pressure,
        MIN(pressure) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_pressure,
        MAX(pressure) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_pressure
    FROM sessionized
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        firmware,
        running_count_pressure,
        LAG(pressure, 2) OVER w_running prev_pressure,
        MAX(session_no) OVER (PARTITION BY device_code) device_code_max_session_no,
        NTILE(4) OVER (PARTITION BY device_code ORDER BY running_count_pressure DESC, id) running_count_pressure_bucket
    FROM with_windows
    WINDOW w_running AS (ORDER BY pressure, id)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        firmware,
        running_count_pressure,
        prev_pressure,
        running_count_pressure_bucket,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY running_count_pressure_bucket, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) status_bits_avg_5,
        SUM(temp_c) OVER (ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_temp_c
    FROM enriched
    WHERE firmware = '1.1.0'
),
with_windows_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        temp_c,
        status_bits,
        firmware,
        prev_pressure,
        status_bits_avg_5,
        running_temp_c,
        LAG(running_count_pressure) OVER w_grp prev_running_count_pressure,
        LEAD(running_count_pressure) OVER w_grp next_running_count_pressure,
        LEAD(status_bits) OVER w_grp next_status_bits,
        ROW_NUMBER() OVER w_grp seq_no
    FROM metrics
    WINDOW w_grp AS (ORDER BY prev_pressure, id)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_temp_c >= 182 THEN 'LARGE' ELSE 'SMALL' END AS running_temp_c_tier,
    CASE WHEN status_bits >= 10 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_bits_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    BIT_OR(status_bits) AS status_bits_bit_or
FROM with_windows_device_readings
GROUP BY CASE WHEN running_temp_c >= 182 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN status_bits >= 10 THEN 'PREMIUM' ELSE 'STANDARD' END

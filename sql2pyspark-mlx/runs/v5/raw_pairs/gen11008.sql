WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CAST(status_bits AS DOUBLE) AS status_bits_num
    FROM device_readings
    WHERE HOUR(reading_ts) BETWEEN 6 AND 13
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM base
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        status_bits,
        firmware,
        status_bits_num,
        SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
-- Grouped metrics
SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    device_code,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS duration_min,
    CORR(temp_c, status_bits) AS corr_temp_c_status_bits,
    BIT_AND(status_bits) AS status_bits_bit_and,
    GROUPING_ID() AS gid
FROM islands
GROUP BY CUBE (DATE_TRUNC('DAY', reading_ts), device_code)

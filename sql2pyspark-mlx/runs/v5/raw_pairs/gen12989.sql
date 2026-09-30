WITH metrics AS (
    SELECT
        *,
        LAST_VALUE(temp_c, TRUE) OVER (PARTITION BY device_code ORDER BY temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_temp_c,
        LAG(temp_c, 2) OVER (ORDER BY temp_c ASC, id ASC) AS prev_temp_c,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS status_bits_avg_2_prior,
        LEAD(humidity, 2) OVER w_hist AS next_humidity,
        SUM(pressure) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS pressure_sum_15m
    FROM device_readings
    WHERE reading_ts < '2024-08-01'
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC)
),
with_prev AS (
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) AS prev_reading_ts
    FROM metrics
),
gaps AS (
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
gaps_2 AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) > INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM islands
),
sessionized AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        status_bits,
        status_bits_avg_2_prior,
        next_humidity,
        prev_reading_ts,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps_2
),
with_gaps AS (
    SELECT *, CASE WHEN prev_reading_ts - LAG(prev_reading_ts) OVER (PARTITION BY device_code ORDER BY prev_reading_ts ASC, id ASC) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM sessionized
),
islands_device_readings AS (
    SELECT
        id,
        device_code,
        temp_c,
        next_humidity,
        prev_reading_ts,
        SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY prev_reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    id,
    device_code,
    temp_c,
    next_humidity,
    session_id,
    CASE WHEN device_code RLIKE '^(DV-02|DV-01)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS device_code_zone
FROM islands_device_readings
ORDER BY session_id DESC NULLS LAST, id ASC
LIMIT 10;

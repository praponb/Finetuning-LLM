WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM device_readings
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM sessionized
),
sessionized_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM gaps
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY new_group_flag DESC) AS dedup_rank
        FROM sessionized_device_readings
    ) x
    WHERE dedup_rank = 1
),
gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM latest
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps_2
)
-- Window metrics over several different partitions
SELECT
    *,
    LAG(burst_id_2, 1, 0) OVER (PARTITION BY device_code ORDER BY humidity, id) AS prev_burst_id_2,
    PERCENT_RANK() OVER (PARTITION BY device_code ORDER BY session_id) AS session_id_percent_rank,
    CORR(temp_c, pressure) OVER (PARTITION BY device_code) AS corr_temp_c_pressure
FROM islands

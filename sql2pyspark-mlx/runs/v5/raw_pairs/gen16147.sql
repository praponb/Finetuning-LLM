WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits ASC, id ASC) AS rn
    FROM device_readings
    QUALIFY rn = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM latest
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(temp_c) OVER (PARTITION BY device_code ORDER BY pressure ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_temp_c,
        AVG(temp_c) OVER (PARTITION BY device_code ORDER BY pressure ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_temp_c,
        MIN(temp_c) OVER (PARTITION BY device_code ORDER BY pressure ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_temp_c
    FROM sessions_device_readings
    WHERE TO_DATE(reading_ts) >= '2024-01-01'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(is_new_session) OVER (PARTITION BY device_code ORDER BY session_id ASC, id ASC) AS next_is_new_session,
        LAG(is_new_session) OVER (PARTITION BY device_code ORDER BY session_id ASC, id ASC) AS prev_is_new_session,
        LEAD(running_count_temp_c) OVER (PARTITION BY device_code ORDER BY session_id ASC, id ASC) AS next_running_count_temp_c,
        LAG(running_count_temp_c) OVER (PARTITION BY device_code ORDER BY session_id ASC, id ASC) AS prev_running_count_temp_c,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY session_id ASC, id ASC) AS row_seq
    FROM with_windows
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY rn DESC, id ASC) = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        device_code,
        pressure,
        running_min_temp_c,
        running_avg_temp_c
    FROM deduped
) src
UNPIVOT (
    reading FOR metric_name IN (pressure, running_min_temp_c, running_avg_temp_c)
)

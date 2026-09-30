WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM device_readings
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM sessions_device_readings
    QUALIFY RANK() OVER (PARTITION BY device_code ORDER BY pressure DESC NULLS LAST) = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY island_id, id) row_seq,
        FIRST_VALUE(pressure, TRUE) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_pressure
    FROM ranked
    WHERE device_code IN ('DV-03', 'DV-02')
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM with_windows
),
sessions_device_readings_2 AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        firmware,
        island_id,
        row_seq,
        SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        firmware,
        device_code,
        session_id
    FROM sessions_device_readings_2
) src
PIVOT (
    SUM(session_id)
    FOR device_code IN ('DV-03' AS dv_03, 'DV-01' AS dv_01)
)

WITH cleaned AS (
    SELECT *, TRIM(device_code) AS clean_device_code
    FROM device_readings
    WHERE device_code = 'DV-02'
),
ranked AS (
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) = 1
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM ranked
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
flagged AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM islands
),
sessions_device_readings AS (
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        firmware,
        session_no,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
deduped AS (
    SELECT
        id,
        reading_ts,
        humidity,
        pressure
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS LAST, id) AS row_num
        FROM sessions_device_readings
    ) t
    WHERE row_num = 1
),
filtered AS (
    SELECT *, pressure + humidity AS pressure_plus_humidity
    FROM deduped
    WHERE reading_ts >= TO_TIMESTAMP('2025-09-01 07:00:00')
)
SELECT
    id,
    humidity,
    pressure,
    pressure_plus_humidity
FROM filtered
ORDER BY pressure_plus_humidity DESC NULLS FIRST, id

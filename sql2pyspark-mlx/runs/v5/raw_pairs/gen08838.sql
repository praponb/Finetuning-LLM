WITH base AS (
    SELECT
        *,
        ROUND(humidity / pressure, 2) humidity_per_pressure,
        CAST(temp_c AS INT) temp_c_int
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
flagged AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM islands
),
islands_device_readings AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
latest AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        session_start_flag,
        is_new_burst,
        burst_id
    FROM islands_device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY is_new_burst DESC NULLS FIRST, id) = 1
)
SELECT
    id,
    kpi,
    val
FROM (
    SELECT
        id,
        pressure,
        humidity,
        temp_c
    FROM latest
) src
UNPIVOT INCLUDE NULLS (
    val FOR kpi IN (pressure AS PRESSURE, humidity, temp_c)
);

WITH base AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        COALESCE(temp_c, 0) AS temp_c_filled
    FROM device_readings
    WHERE reading_ts >= TO_DATE('2025-09-01')
),
gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM base
),
sessions_device_readings AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
deduped AS (
    SELECT
        id,
        humidity,
        pressure,
        temp_c_filled,
        is_new_session,
        burst_id
    FROM sessions_device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY is_new_session DESC, id) = 1
),
cleaned AS (
    SELECT
        id,
        humidity,
        pressure,
        temp_c_filled,
        is_new_session,
        burst_id,
        temp_c_filled + humidity AS temp_c_filled_plus_humidity
    FROM deduped
    WHERE humidity > 53
),
kept_keys AS (
    SELECT id
    FROM cleaned
    EXCEPT
    SELECT id
    FROM device_readings
    WHERE firmware IN ('2.0.1', '1.1.0', '1.0.3')
),
kept_rows AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.id = k.id
),
daily AS (
    SELECT
        burst_id,
        ROUND(AVG(is_new_session), 2) AS avg_is_new_session,
        COUNT(DISTINCT id) AS distinct_ids
    FROM kept_rows
    GROUP BY burst_id
    HAVING ROUND(AVG(is_new_session), 2) > 0
)
SELECT
    burst_id,
    avg_is_new_session,
    distinct_ids
FROM daily
ORDER BY avg_is_new_session DESC NULLS FIRST;

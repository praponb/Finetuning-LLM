WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM device_readings
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM sessions_device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY session_id, id) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(session_id) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS session_id_count_30m,
        AVG(session_id) OVER w_running AS session_id_avg_2,
        NTILE(5) OVER w_ordered AS session_start_flag_bucket
    FROM latest
    WINDOW w_running AS (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_ordered AS (ORDER BY session_start_flag DESC, id)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        session_start_flag,
        session_id_avg_2,
        CAST(reading_ts AS DATE) AS reading_ts_day
    FROM scored
    WHERE humidity > 36
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    device_code,
    metric,
    value
FROM (
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        humidity
    FROM cleaned
) src
UNPIVOT (
    value FOR metric IN (temp_c, pressure, humidity)
);

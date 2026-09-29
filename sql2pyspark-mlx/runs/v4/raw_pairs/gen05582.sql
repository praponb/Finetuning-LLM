WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS LAST, id ASC) = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM deduped
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        is_new_session,
        burst_id,
        LAG(pressure, 1) OVER (PARTITION BY device_code ORDER BY temp_c ASC, id ASC) AS prev_pressure,
        LEAD(burst_id, 1, 0) OVER (PARTITION BY device_code ORDER BY is_new_session ASC, id ASC) AS next_burst_id,
        LAST_VALUE(humidity, TRUE) OVER (PARTITION BY device_code ORDER BY temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_humidity
    FROM sessions_device_readings
),
gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_session_2
    FROM scored
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session_2) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps_2
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN is_new_session >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_new_session_tier,
        CASE WHEN pressure >= 1024 THEN 'PREMIUM' ELSE 'STANDARD' END AS pressure_tier,
        CASE WHEN prev_pressure >= 1030 THEN 'HIGH' ELSE 'LOW' END AS prev_pressure_tier,
        COUNT(CASE WHEN device_code = 'DV-03' THEN 1 END) AS dv_03_count,
        SUM(prev_pressure) FILTER (WHERE device_code = 'DV-02') AS dv_02_total_prev_pressure,
        SUM(CASE WHEN device_code = 'DV-03' THEN is_new_session ELSE 0 END) AS dv_03_is_new_session,
        COUNT(*) AS row_count
    FROM islands
    GROUP BY is_new_session_tier, pressure_tier, prev_pressure_tier
)
SELECT
    is_new_session_tier,
    pressure_tier,
    prev_pressure_tier,
    dv_03_count,
    dv_02_total_prev_pressure,
    row_count
FROM summary;

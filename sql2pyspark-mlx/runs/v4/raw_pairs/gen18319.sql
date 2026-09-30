WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC NULLS FIRST, id) AS rn
    FROM device_readings
    QUALIFY rn = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(humidity) OVER (PARTITION BY device_code ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_humidity,
        NTILE(5) OVER (PARTITION BY device_code ORDER BY pressure, id) AS pressure_bucket,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY rn DESC, id) AS seq_no
    FROM latest
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM metrics
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions_device_readings
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no_2
    FROM with_gaps
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN pressure >= 1016 THEN 'High' WHEN pressure >= 999 THEN 'Medium' ELSE 'Low' END AS pressure_band,
        ROUND(session_no_2 / seq_no, 2) AS session_no_2_per_seq_no
    FROM sessionized
    WHERE reading_ts > TIMESTAMP '2025-09-01 16:00:00'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS session_start_flag_2
    FROM filtered
),
sessionized_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag_2) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY reading_ts DESC) AS rn_2
        FROM sessionized_device_readings
    ) ranked
    WHERE rn_2 = 1
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            firmware,
            device_code,
            session_no_2_per_seq_no
        FROM ranked
    ) src
    PIVOT (
        MAX(session_no_2_per_seq_no)
        FOR device_code IN ('DV-01' AS dv_01, 'DV-03' AS dv_03)
    )
)
SELECT
    firmware,
    dv_01,
    dv_03
FROM wide
ORDER BY dv_03 DESC;

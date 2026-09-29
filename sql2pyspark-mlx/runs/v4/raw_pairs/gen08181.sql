WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS row_num
    FROM device_readings
    QUALIFY row_num = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY temp_c DESC NULLS FIRST) AS temp_c_dense_rank,
        AVG(pressure) OVER w_running AS device_code_avg_pressure
    FROM deduped
    WINDOW w_running AS (PARTITION BY device_code)
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM metrics
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(temp_c * 1.07, 2) AS temp_c_scaled
    FROM sessionized
    WHERE reading_ts > TO_DATE('2025-09-01')
      AND device_code_avg_pressure < 1003
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    status_bits,
    firmware,
    row_num
FROM filtered

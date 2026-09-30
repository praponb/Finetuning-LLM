WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(status_bits, 1) AS status_bits_rounded,
        CONCAT(firmware, ' ', device_code) AS firmware_device_code
    FROM device_readings
    WHERE firmware IN ('1.0.3', '2.0.1', '1.1.0')
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM sessionized
),
sessionized_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        status_bits_rounded,
        firmware_device_code,
        session_id,
        is_new_session,
        session_no,
        GREATEST(pressure, session_start_flag) AS greatest_pressure_session_start_flag
    FROM sessionized_device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
base_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(device_code) AS device_code_upper
    FROM filtered
    WHERE reading_ts > TO_DATE('2025-09-01')
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('HOUR', reading_ts) AS hour_window,
        device_code,
        ROUND(AVG(is_new_session), 2) AS avg_is_new_session,
        COUNT(DISTINCT firmware) AS distinct_firmwares,
        CORR(pressure, status_bits) AS corr_pressure_status_bits,
        MIN(reading_ts) AS started_at,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('HOUR', reading_ts) ORDER BY ROUND(AVG(is_new_session), 2) DESC) AS avg_is_new_session_percent_rank
    FROM base_device_readings
    GROUP BY DATE_TRUNC('HOUR', reading_ts), device_code
    HAVING ROUND(AVG(is_new_session), 2) >= 0
)
SELECT
    hour_window,
    device_code,
    avg_is_new_session,
    distinct_firmwares,
    corr_pressure_status_bits,
    started_at,
    avg_is_new_session_percent_rank
FROM grouped
ORDER BY corr_pressure_status_bits DESC NULLS FIRST;

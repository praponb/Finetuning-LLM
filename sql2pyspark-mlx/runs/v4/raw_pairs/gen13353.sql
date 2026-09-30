WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(pressure * 0.9, 2) AS adjusted_pressure
    FROM device_readings
    WHERE humidity < 75
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Grouped metrics
SELECT
    CASE WHEN session_no >= 8 THEN 'HIGH' ELSE 'LOW' END AS session_no_tier,
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    CASE WHEN status_bits >= 12 THEN 'HIGH' ELSE 'LOW' END AS status_bits_tier,
    MAX(pressure) AS max_pressure,
    PERCENTILE_APPROX(is_new_session, 0.9) AS is_new_session_p90,
    BIT_AND(status_bits) AS status_bits_bit_and,
    MIN(reading_ts) AS started_at,
    ROUND(SUM(adjusted_pressure) / NULLIF(COUNT(CASE WHEN device_code = 'DV-03' THEN 1 END), 0), 2) AS adjusted_pressure_per_dv_03
FROM sessionized
GROUP BY ROLLUP (CASE WHEN session_no >= 8 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('HOUR', reading_ts), CASE WHEN status_bits >= 12 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY hour_window NULLS LAST

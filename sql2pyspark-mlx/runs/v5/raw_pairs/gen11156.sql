WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM device_readings
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Grouped metrics
SELECT
    device_code,
    COUNT_IF(device_code = 'DV-03') AS dv_03_events,
    SUM(CASE WHEN device_code = 'DV-01' THEN status_bits ELSE 0 END) AS dv_01_status_bits,
    BIT_OR(status_bits) AS status_bits_bit_or,
    PERCENTILE_APPROX(status_bits, 0.9) AS status_bits_p90,
    APPROX_PERCENTILE(new_group_flag, 0.9) AS new_group_flag_p90,
    COUNT(*) AS cnt
FROM islands
GROUP BY 1

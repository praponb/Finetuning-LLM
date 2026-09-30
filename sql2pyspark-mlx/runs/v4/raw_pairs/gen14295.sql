WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM device_readings
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, device_code
    FROM sessions_device_readings
    EXCEPT
    SELECT id, device_code
    FROM device_readings
    WHERE humidity <= 75
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions_device_readings s
    JOIN matched_keys k ON s.id = k.id AND s.device_code = k.device_code
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM kept_rows
    EXCEPT ALL
    SELECT id
    FROM device_readings
    WHERE device_code = 'DV-04'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    JOIN kept_keys k ON s.id = k.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    APPROX_PERCENTILE(pressure, ARRAY(0.25, 0.5, 0.75)) AS pressure_quartiles,
    ROUND(SUM(temp_c) / NULLIF(COUNT(CASE WHEN device_code = 'DV-02' THEN 1 END), 0), 2) AS temp_c_per_dv_02
FROM reconciled
GROUP BY CUBE (DATE_TRUNC('HOUR', reading_ts), DATE_TRUNC('DAY', reading_ts))

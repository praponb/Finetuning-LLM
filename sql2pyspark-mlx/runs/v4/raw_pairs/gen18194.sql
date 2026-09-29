WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, temp_c + humidity AS temp_c_plus_humidity
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
      AND HOUR(reading_ts) BETWEEN 12 AND 18
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    device_code,
    SUM(CASE WHEN device_code = 'DV-04' THEN status_bits ELSE 0 END) AS dv_04_status_bits,
    COUNT(*) AS event_count,
    MIN(reading_ts) AS started_at,
    COUNT(*) FILTER (WHERE device_code = 'DV-03') AS dv_03_rows,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS span_minutes
FROM islands
GROUP BY device_code
HAVING span_minutes >= 0;

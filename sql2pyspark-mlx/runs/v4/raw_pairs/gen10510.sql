WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM device_readings
)
-- Running sum of the flag = session number
SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
FROM flagged;

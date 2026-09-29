WITH valid AS (
    SELECT *, CAST(status_bits AS STRING) AS status_bits_num
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 03:00:00'
),
with_prev AS (
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM valid
),
flagged AS (
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessions_device_readings AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    CASE WHEN humidity >= 74 THEN 'HIGH' ELSE 'LOW' END AS humidity_tier,
    ROUND(SUM(humidity) / NULLIF(COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END), 0), 2) AS humidity_per_dv_04,
    COUNT_IF(device_code = 'DV-02') AS dv_02_events
FROM sessions_device_readings
GROUP BY CASE WHEN humidity >= 74 THEN 'HIGH' ELSE 'LOW' END

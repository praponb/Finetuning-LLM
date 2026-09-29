WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(temp_c AS INT) AS temp_c_int
    FROM device_readings
    WHERE status_bits = 10
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '1' HOUR THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    MIN(reading_ts) AS first_seen,
    SUM(new_group_flag) FILTER (WHERE device_code = 'DV-04') AS dv_04_total_new_group_flag
FROM sessionized
GROUP BY 1

WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id ASC) AS dedup_rank
    FROM device_readings
    QUALIFY dedup_rank = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    COUNT_IF(device_code = 'DV-04') AS dv_04_events,
    PERCENTILE_APPROX(humidity, 0.9) AS humidity_p90,
    MAX(reading_ts) AS last_seen,
    COUNT(*) FILTER (WHERE device_code = 'DV-03') AS dv_03_rows,
    ROUND(AVG(humidity), 2) AS avg_humidity
FROM ranked
GROUP BY DATE_TRUNC('HOUR', reading_ts);

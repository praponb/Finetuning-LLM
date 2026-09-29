WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS LAST, id) AS dedup_rank
        FROM device_readings
    ) ranked
    WHERE dedup_rank = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    PERCENTILE_APPROX(humidity, 0.5) AS humidity_p50,
    SUM(CASE WHEN device_code = 'DV-04' THEN humidity ELSE 0 END) AS dv_04_humidity,
    COUNT(*) AS row_count,
    COUNT(*) FILTER (WHERE device_code = 'DV-02') AS dv_02_rows
FROM deduped
GROUP BY 1, 2
ORDER BY dv_04_humidity DESC NULLS FIRST;

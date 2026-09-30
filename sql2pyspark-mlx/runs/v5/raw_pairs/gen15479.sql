WITH gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM device_readings
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    device_code,
    CASE WHEN new_group_flag >= 1 THEN 'HIGH' ELSE 'LOW' END AS new_group_flag_tier,
    ROUND(AVG(status_bits), 2) AS avg_status_bits,
    AVG(pressure) AS avg_pressure,
    COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END) AS dv_01_count,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS span_minutes,
    PERCENT_RANK() OVER (PARTITION BY device_code ORDER BY COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END) DESC) AS dv_01_count_percent_rank
FROM islands
GROUP BY 1, 2
HAVING COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END) > 1;

WITH flagged AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM device_readings
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    CASE WHEN pressure >= 1019 THEN 'PREMIUM' ELSE 'STANDARD' END AS pressure_tier,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    COUNT_IF(device_code = 'DV-03') AS dv_03_events,
    STDDEV_SAMP(temp_c) AS temp_c_stddev,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('DAY', reading_ts) ORDER BY COUNT_IF(device_code = 'DV-03') DESC) AS dv_03_events_percent_rank
FROM sessionized
GROUP BY 1, 2
ORDER BY temp_c_stddev

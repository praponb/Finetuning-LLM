WITH scored AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        LAG(temp_c, 1) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_temp_c,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c DESC, id) AS row_seq,
        CORR(humidity, status_bits) OVER (PARTITION BY device_code) AS corr_humidity_status_bits,
        SUM(temp_c) OVER (ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_temp_c
    FROM device_readings
)
SELECT
    device_code,
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    CASE WHEN humidity >= 72 THEN 'LARGE' ELSE 'SMALL' END AS humidity_tier,
    SUM(temp_c) AS total_temp_c,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS duration_min,
    AVG(humidity) AS avg_humidity,
    SUM(CASE WHEN device_code = 'DV-02' THEN temp_c ELSE 0 END) AS dv_02_temp_c,
    COUNT(DISTINCT id) AS distinct_ids,
    GROUPING_ID() AS grouping_level
FROM scored
GROUP BY GROUPING SETS ((device_code, DATE_TRUNC('DAY', reading_ts), CASE WHEN humidity >= 72 THEN 'LARGE' ELSE 'SMALL' END), (device_code, DATE_TRUNC('DAY', reading_ts)))
HAVING COUNT(DISTINCT id) < 1
ORDER BY activity_day DESC NULLS FIRST

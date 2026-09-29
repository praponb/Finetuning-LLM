SELECT
    id,
    reading_ts,
    humidity,
    pressure,
    LAST_VALUE(humidity) IGNORE NULLS OVER (ORDER BY humidity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_humidity,
    COUNT(humidity) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS humidity_count_30m,
    NTILE(3) OVER (PARTITION BY device_code ORDER BY humidity DESC, id ASC) AS humidity_bucket,
    ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id ASC) AS row_seq
FROM device_readings;

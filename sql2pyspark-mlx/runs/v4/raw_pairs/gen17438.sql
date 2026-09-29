WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(temp_c) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS temp_c_avg_30m,
        PERCENT_RANK() OVER (PARTITION BY device_code ORDER BY temp_c DESC NULLS LAST) AS temp_c_percent_rank,
        NTILE(10) OVER (PARTITION BY device_code ORDER BY pressure, id) AS pressure_bucket
    FROM device_readings
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    pressure,
    status_bits,
    firmware,
    temp_c_avg_30m,
    temp_c_percent_rank
FROM with_windows
ORDER BY reading_ts NULLS LAST;

WITH latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS FIRST, id) AS row_num
        FROM device_readings
    ) x
    WHERE row_num = 1
)
SELECT
    device_code,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS span_minutes,
    MIN(reading_ts) AS first_seen,
    APPROX_PERCENTILE(temp_c, ARRAY(0.25, 0.5, 0.75)) AS temp_c_quartiles
FROM latest
GROUP BY 1;
